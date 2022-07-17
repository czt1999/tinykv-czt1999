package raftstore

import (
	"fmt"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/raft"
	"time"

	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
)

type PeerTick int

const (
	PeerTickRaft               PeerTick = 0
	PeerTickRaftLogGC          PeerTick = 1
	PeerTickSplitRegionCheck   PeerTick = 2
	PeerTickSchedulerHeartbeat PeerTick = 3
)

type peerMsgHandler struct {
	*peer
	ctx          *GlobalContext
	splitNewPeer *peer
}

func newPeerMsgHandler(peer *peer, ctx *GlobalContext) *peerMsgHandler {
	return &peerMsgHandler{
		peer: peer,
		ctx:  ctx,
	}
}

func (d *peerMsgHandler) checkApplied(rd *raft.Ready) (map[uint64]*pb.Entry, *raft_cmdpb.RaftCmdRequest) {
	applied := make(map[uint64]*pb.Entry, 0)
	var split *raft_cmdpb.RaftCmdRequest
	for i := range rd.CommittedEntries {
		ent := rd.CommittedEntries[i]
		applied[ent.Index] = &ent
		if len(ent.Data) == 0 {
			continue
		}
		// check conf change
		needDestroy := d.mayApplyConfChange(&ent)
		if needDestroy {
			d.destroyPeer()
			break
		}
		// check split
		cmd := &raft_cmdpb.RaftCmdRequest{}
		if err := cmd.Unmarshal(ent.Data); err == nil {
			if cmd.AdminRequest != nil && cmd.AdminRequest.Split != nil {
				split = cmd
				//d.mayApplySplit(split)
			}
		}
	}
	return applied, split
}

func (d *peerMsgHandler) newCmdRespFromReq(cmd *raft_cmdpb.RaftCmdRequest, cb *message.Callback) *raft_cmdpb.RaftCmdResponse {
	req := cmd.Requests[0]
	resp := newCmdResp()
	resp.Header.CurrentTerm = d.Term()
	resp.Responses = []*raft_cmdpb.Response{{CmdType: req.CmdType}}
	switch req.CmdType {
	case raft_cmdpb.CmdType_Get:
		resp.Responses[0].Get = &raft_cmdpb.GetResponse{}
		val, err := engine_util.GetCF(d.peerStorage.Engines.Kv, engine_util.CfDefault, req.Get.Key)
		if err != nil {
			log.Errorf("Request Get error %v", err)
			return resp
		}
		resp.Responses[0].Get.Value = val
	case raft_cmdpb.CmdType_Snap:
		region := d.Region()
		if cmd.Header.RegionEpoch.Version != region.RegionEpoch.Version {
			return ErrResp(&util.ErrEpochNotMatch{Regions: []*metapb.Region{region}})
		}
		resp.Responses[0].Snap = &raft_cmdpb.SnapResponse{
			Region: &metapb.Region{
				Id:       region.Id,
				StartKey: region.StartKey,
				EndKey:   region.EndKey,
			},
		}
		cb.Txn = d.peerStorage.Engines.Kv.NewTransaction(false)
	case raft_cmdpb.CmdType_Put:
		resp.Responses[0].Put = &raft_cmdpb.PutResponse{}
	case raft_cmdpb.CmdType_Delete:
		resp.Responses[0].Delete = &raft_cmdpb.DeleteResponse{}
	}
	return resp
}

func (d *peerMsgHandler) addRegionPeer(peerId uint64, storeId uint64) {
	newPeer := &metapb.Peer{
		Id:      peerId,
		StoreId: storeId,
	}
	d.Region().Peers = append(d.Region().Peers, newPeer)
	d.Region().RegionEpoch.ConfVer += 1
	d.insertPeerCache(newPeer)
}

func (d *peerMsgHandler) removeRegionPeer(peerId uint64) {
	newPeers := make([]*metapb.Peer, 0)
	for _, p := range d.Region().Peers {
		if p.Id != peerId {
			newPeers = append(newPeers, p)
		}
	}
	d.Region().Peers = newPeers
	d.Region().RegionEpoch.ConfVer += 1
	d.removePeerCache(peerId)
}

func (d *peerMsgHandler) writeRegionState() {
	kvWB := new(engine_util.WriteBatch)
	regionState := &rspb.RegionLocalState{
		State:  rspb.PeerState_Normal,
		Region: d.Region(),
	}
	_ = kvWB.SetMeta(meta.RegionStateKey(d.regionId), regionState)
	kvWB.MustWriteToDB(d.peerStorage.Engines.Kv)
}

func (d *peerMsgHandler) updateStoreMeta() {
	sm := d.ctx.storeMeta
	sm.Lock()
	sm.regions[d.regionId] = d.Region()
	sm.regionRanges.ReplaceOrInsert(&regionItem{region: d.Region()})
	sm.Unlock()
}

func (d *peerMsgHandler) saveRegionState(region *metapb.Region) {
	kvWB := new(engine_util.WriteBatch)
	regionState := &rspb.RegionLocalState{State: rspb.PeerState_Normal, Region: region}
	err := kvWB.SetMeta(meta.RegionStateKey(region.Id), regionState)
	if err != nil {
		panic(err)
	}
	err = d.peerStorage.Engines.WriteKV(kvWB)
	if err != nil {
		panic(err)
	}
}

func (d *peerMsgHandler) mayApplyConfChange(ent *pb.Entry) bool {
	if ent.EntryType != pb.EntryType_EntryConfChange {
		return false
	}
	cc := pb.ConfChange{}
	if err := cc.Unmarshal(ent.Data); err != nil {
		log.Panicf("ConfChange Unmarshall error %v", err)
	}
	req := raft_cmdpb.RaftCmdRequest{}
	if err := req.Unmarshal(cc.Context); err != nil {
		log.Panicf("ConfChangeRequest Unmarshall error %v", err)
	}
	if util.IsEpochStale(req.Header.RegionEpoch, d.Region().RegionEpoch) {
		//if req.Header.RegionEpoch.ConfVer != d.Region().RegionEpoch.ConfVer {
		log.Warnf("%v ignore stale conf change (%v/%v)", d.Tag, req.Header.RegionEpoch, d.Region().RegionEpoch)
		for _, proposal := range d.proposals {
			if proposal.index == ent.Index && proposal.term == ent.Term {
				proposal.cb.Done(ErrResp(&util.ErrEpochNotMatch{Regions: []*metapb.Region{d.Region()}}))
			}
		}
		return false
	}
	log.Warnf("[%v] apply conf change (index %v epoch %v) %v", d.PeerId(), ent.Index, d.Region().RegionEpoch.ConfVer+1, cc)
	switch cc.ChangeType {
	case pb.ConfChangeType_AddNode:
		d.addRegionPeer(cc.NodeId, req.AdminRequest.ChangePeer.Peer.StoreId)
	case pb.ConfChangeType_RemoveNode:
		d.removeRegionPeer(cc.NodeId)
		if cc.NodeId == d.Meta.Id {
			return true
		}
	}
	d.writeRegionState()
	d.updateStoreMeta()
	_ = d.RaftGroup.ApplyConfChange(cc)
	log.Warnf("[%v] apply conf change (index %v epoch %v) -> %v", d.PeerId(), ent.Index, d.Region().RegionEpoch.ConfVer, d.RaftGroup.Raft.Prs)
	// callback
	for _, proposal := range d.proposals {
		if proposal.index == ent.Index && proposal.term == ent.Term {
			resp := newCmdResp()
			resp.AdminResponse = &raft_cmdpb.AdminResponse{CmdType: raft_cmdpb.AdminCmdType_ChangePeer}
			proposal.cb.Done(resp)
		}
	}
	return false
}

func (d *peerMsgHandler) mayApplySplit(splCmd *raft_cmdpb.RaftCmdRequest) {
	if util.IsEpochStale(splCmd.Header.RegionEpoch, d.Region().RegionEpoch) {
		return
	}
	spl := splCmd.AdminRequest.Split
	endKey := d.Region().EndKey
	confVer := d.Region().RegionEpoch.ConfVer
	version := d.Region().RegionEpoch.Version
	// update region info :
	//     one will inherit the metadata before splitting and just modify its Range and RegionEpoch
	//     while the other will create relevant meta information
	if engine_util.ExceedEndKey(spl.SplitKey, endKey) ||
		engine_util.ExceedEndKey(d.Region().StartKey, spl.SplitKey) {
		return
	}
	d.Region().EndKey = spl.SplitKey
	d.Region().RegionEpoch.Version += 1
	newRegion := &metapb.Region{
		Id:          spl.NewRegionId,
		StartKey:    spl.SplitKey,
		EndKey:      endKey,
		RegionEpoch: &metapb.RegionEpoch{ConfVer: confVer, Version: version + 1},
		Peers:       make([]*metapb.Peer, len(spl.NewPeerIds)),
	}
	for i, p := range d.Region().Peers {
		newRegion.Peers[i] = &metapb.Peer{
			Id:      spl.NewPeerIds[i],
			StoreId: p.StoreId,
		}
	}
	// create peer
	newPeer, err := createPeer(d.storeID(), d.ctx.cfg, d.ctx.regionTaskSender, d.ctx.engine, newRegion)
	if err != nil {
		log.Panicf("create peer err %v", err)
	}
	// register new peer in router and make it start
	d.ctx.router.register(newPeer)
	_ = d.ctx.router.send(newRegion.Id, message.Msg{RegionID: newRegion.Id, Type: message.MsgTypeStart})
	d.splitNewPeer = newPeer
	// update metadata in ctx
	sm := d.ctx.storeMeta
	sm.Lock()
	sm.regionRanges.ReplaceOrInsert(&regionItem{region: newRegion})
	sm.regions[newRegion.Id] = newRegion
	sm.Unlock()
	// save region state
	d.saveRegionState(d.Region())
	d.saveRegionState(newRegion)
}

func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}
	// Your Code Here (2B).
	rd := d.RaftGroup.Ready()
	prevTruncatedIndex := d.peerStorage.applyState.TruncatedState.Index
	// persist states and entries and apply entries if any
	applySnapResult, err := d.peerStorage.SaveReadyState(&rd)
	if err != nil {
		panic(err)
	}
	// conf change applied by snapshot
	if applySnapResult.PrevRegion.RegionEpoch.ConfVer != applySnapResult.Region.RegionEpoch.ConfVer {
		d.updateStoreMeta()
	}
	// index applied
	applied, splitCmd := d.checkApplied(&rd)
	if d.stopped {
		return
	}
	remainedProposals := make([]*proposal, 0)
	for _, proposal := range d.proposals {
		if ent, ok := applied[proposal.index]; ok && ent.Term == proposal.term {
			cmd := &raft_cmdpb.RaftCmdRequest{}
			if err = cmd.Unmarshal(ent.Data); err == nil {
				if len(cmd.Requests) > 0 {
					resp := d.newCmdRespFromReq(cmd, proposal.cb)
					proposal.cb.Done(resp)
					continue
				}
			}
		} else if ok && ent.Term != proposal.term {
			// leader may have been changed
			regionID := d.regionId
			leaderID := d.LeaderId()
			leader := d.getPeerFromCache(leaderID)
			proposal.cb.Done(ErrResp(&util.ErrNotLeader{RegionId: regionID, Leader: leader}))
		} else {
			remainedProposals = append(remainedProposals, proposal)
		}
	}
	d.proposals = remainedProposals
	// split
	if splitCmd != nil {
		d.mayApplySplit(splitCmd)
	}
	// send messages if any
	for _, msg := range rd.Messages {
		_ = d.sendRaftMessage(msg, d.ctx.trans)
	}
	// schedule gc if necessary
	truncatedIndex := d.peerStorage.applyState.TruncatedState.Index
	if len(rd.CommittedEntries) > 0 && truncatedIndex > prevTruncatedIndex {
		d.ScheduleCompactLog(truncatedIndex)
	}
	d.RaftGroup.Advance(rd)
}

func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	switch msg.Type {
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v", d.Tag, err)
		}
	case message.MsgTypeRaftCmd:
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	case message.MsgTypeTick:
		d.onTick()
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		log.Infof("%s on split with %v", d.Tag, split.SplitKey)
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap:
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart:
		d.startTicker()
	}
}

func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		return err
	}

	// Check whether the store has the right peer to handle the request.
	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	// Check whether the term is stale.
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	return err
}

func (d *peerMsgHandler) checkCommandRegion(msg *raft_cmdpb.RaftCmdRequest) error {
	if len(msg.Requests) > 0 {
		var key []byte
		switch msg.Requests[0].CmdType {
		case raft_cmdpb.CmdType_Get:
			key = msg.Requests[0].Get.Key
		case raft_cmdpb.CmdType_Put:
			key = msg.Requests[0].Put.Key
		case raft_cmdpb.CmdType_Delete:
			key = msg.Requests[0].Delete.Key
		default:
			return nil
		}
		return util.CheckKeyInRegion(key, d.Region())
	}
	return nil
}

func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	//Your Code Here (2B).
	//log.Debugf("%v proposeRaftCommand %v", d.Tag, msg)

	if err = d.checkCommandRegion(msg); err != nil {
		log.Debugf("%v check region err", err)
		cb.Done(ErrResp(err))
		return
	}

	if d.RaftGroup.Raft.LeadTransfering() {
		log.Debugf("%v Lead transfering", d.Tag)
		return
	}

	if len(msg.Requests) > 0 {
		d.mustPropose(msg, cb)
		return
	}

	if msg.AdminRequest != nil {
		switch msg.AdminRequest.CmdType {
		case raft_cmdpb.AdminCmdType_TransferLeader:
			d.handleTransferLeader(msg, cb)
		case raft_cmdpb.AdminCmdType_ChangePeer:
			d.handleChangePeer(msg, cb)
		case raft_cmdpb.AdminCmdType_Split:
			d.handleSplit(msg, cb)
		default:
			d.mustPropose(msg, cb)
		}
	}
}

func (d peerMsgHandler) mustPropose(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	data, err := msg.Marshal()
	if err != nil {
		log.Panicf("msg marshal error %v", err)
	}
	d.appendNewProposal(cb)
	if err = d.RaftGroup.Propose(data); err != nil {
		log.Panicf("msg propose error %v", err)
	}
}

func (d *peerMsgHandler) appendNewProposal(cb *message.Callback) {
	d.proposals = append(d.proposals, &proposal{
		index: d.nextProposalIndex(),
		term:  d.Term(),
		cb:    cb,
	})
}

func (d *peerMsgHandler) handleTransferLeader(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	tl := msg.AdminRequest.TransferLeader
	log.Warnf("AdminCmdType_TransferLeader %v -> %v", d.PeerId(), tl.Peer.Id)
	// TransferLeader is an action with no need to be replicated
	d.RaftGroup.TransferLeader(tl.Peer.Id)
	resp := newCmdResp()
	resp.AdminResponse = &raft_cmdpb.AdminResponse{
		CmdType: raft_cmdpb.AdminCmdType_TransferLeader,
	}
	// response directly
	cb.Done(resp)
}

func (d *peerMsgHandler) handleChangePeer(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	cp := msg.AdminRequest.ChangePeer
	if d.RaftGroup.Raft.PendingConfIndex > d.peerStorage.AppliedIndex() {
		cb.Done(ErrResp(errors.New("last conf_change is pending")))
		return
	}
	if startPendingTime, ok := d.PeersStartPendingTime[cp.Peer.Id]; ok {
		cb.Done(ErrResp(errors.New("Target peer is pending")))
		log.Warnf("%v target peer %v is pending (%s) ", cp.ChangeType, cp.Peer.Id, startPendingTime)
		return
	}
	if cp.ChangeType == pb.ConfChangeType_RemoveNode && cp.Peer.Id == d.PeerId() {
		// remove leader
		maybeTransferee := d.RaftGroup.Raft.PeerMaybeTransferee()
		if maybeTransferee != raft.None {
			cb.Done(ErrResp(errors.New("target peer is leader, transfer pending")))
			log.Warnf("target peer is leader %v, transfer to %v", d.PeerId(), maybeTransferee)
			d.RaftGroup.TransferLeader(maybeTransferee)
			return
		}
	}
	ctx, _ := msg.Marshal()
	cc := pb.ConfChange{
		ChangeType: cp.ChangeType,
		NodeId:     cp.Peer.Id,
		Context:    ctx,
	}
	d.appendNewProposal(cb)
	_ = d.RaftGroup.ProposeConfChange(cc)
}

func (d *peerMsgHandler) handleSplit(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	if d.splitNewPeer != nil && !d.splitNewPeer.stopped && d.splitNewPeer.LeaderId() == raft.None {
		return
	}
	spl := msg.AdminRequest.Split
	log.Warnf("AdminCmdType_Split %s %v %v", spl.SplitKey, spl.NewRegionId, spl.NewPeerIds)
	d.mustPropose(msg, cb)
}

func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	d.ctx.tickDriverSender <- d.regionId
}

func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionId, d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionId
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

func (d *peerMsgHandler) onRaftBaseTick() {
	d.RaftGroup.Tick()
	d.ticker.schedule(PeerTickRaft)
}

func (d *peerMsgHandler) ScheduleCompactLog(truncatedIndex uint64) {
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     truncatedIndex + 1,
	}
	d.LastCompactedIdx = raftLogGCTask.EndIdx
	d.ctx.raftLogGCTaskSender <- raftLogGCTask
}

func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	//log.Debugf("%s handle raft message %s from %d to %d",
	//	d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	key, err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	if key != nil {
		// If the snapshot file is not used again, then it's OK to
		// delete them here. If the snapshot file will be reused when
		// receiving, then it will fail to pass the check again, so
		// missing snapshot files should not be noticed.
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	d.insertPeerCache(msg.GetFromPeer())
	err = d.RaftGroup.Step(*msg.GetMessage())
	if err != nil {
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return nil
}

// return false means the message is invalid, and can be ignored.
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	//from := msg.GetFromPeer()
	to := msg.GetToPeer()
	//log.Debugf("[region %d] handle raft message %s from %d to %d", regionID, msg, from.GetId(), to.GetId())
	if to.GetStoreId() != d.storeID() {
		log.Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil {
		log.Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

/// Checks if the message is sent to the correct peer.
///
/// Returns true means that the message can be dropped silently.
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with scheduler and scheduler will
	// tell 2 is stale, so 2 can remove itself.
	region := d.Region()
	if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
		// The message is stale and not in current region.
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.PeerId() {
		log.Infof("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId())
		return true
	} else if target.Id > d.PeerId() {
		if d.MaybeDestroy() {
			log.Infof("%s is stale as received a larger peer %s, destroying", d.Tag, target)
			d.destroyPeer()
			d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
		}
		return true
	}
	return false
}

func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.Infof("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msgType, curEpoch)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    toPeer,
		ToPeer:      fromPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Errorf("[region %d] send message failed %v", regionID, err)
	}
}

func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !util.PeerEqual(d.Meta, msg.ToPeer) {
		log.Infof("%s receive stale gc msg, ignore", d.Tag)
		return
	}
	log.Infof("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer)
	if d.MaybeDestroy() {
		d.destroyPeer()
	}
}

// Returns `None` if the `msg` doesn't contain a snapshot or it contains a snapshot which
// doesn't conflict with any other snapshots or regions. Otherwise a `snap.SnapKey` is returned.
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snapshot.Data)
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region
	peerID := msg.ToPeer.Id
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	if !contains {
		log.Infof("%s %s doesn't contains peer %d, skip", d.Tag, snapRegion, peerID)
		return &key, nil
	}
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() {
			log.Infof("%s stale delegate detected, skip", d.Tag)
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		log.Infof("%s region overlapped %s %s", d.Tag, existRegion, snapRegion)
		return &key, nil
	}

	// check if snapshot file exists.
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (d *peerMsgHandler) destroyPeer() {
	log.Infof("%s starts destroy", d.Tag)
	regionID := d.regionId
	// We can't destroy a peer which is applying snapshot.
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	isInitialized := d.isInitialized()
	if err := d.Destroy(d.ctx.engine, false); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.Tag, err))
	}
	d.ctx.router.close(regionID)
	d.stopped = true
	if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		panic(d.Tag + " meta corruption detected")
	}
	if _, ok := meta.regions[regionID]; !ok {
		panic(d.Tag + " meta corruption detected")
	}
	delete(meta.regions, regionID)
}

func (d *peerMsgHandler) findSiblingRegion() (result *metapb.Region) {
	meta := d.ctx.storeMeta
	meta.RLock()
	defer meta.RUnlock()
	item := &regionItem{region: d.Region()}
	meta.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem).region
		return true
	})
	return
}

func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)
	if !d.IsLeader() {
		return
	}

	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()
	var compactIdx uint64
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		compactIdx = appliedIdx
	} else {
		return
	}

	y.Assert(compactIdx > 0)
	compactIdx -= 1
	if compactIdx < firstIdx {
		// In case compact_idx == first_idx before subtraction.
		return
	}

	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Fatalf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx)
		panic(err)
	}

	// Create a compact log request and notify directly.
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
	d.proposeRaftCommand(request, nil)
}

func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}

	if !d.IsLeader() {
		return
	}
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
}

func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
	if len(splitKey) == 0 {
		err := errors.Errorf("%s split key should not be empty", d.Tag)
		log.Error(err)
		return err
	}

	if !d.IsLeader() {
		// region on this store is no longer leader, skipped.
		log.Infof("%s not leader, skip", d.Tag)
		return &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		}
	}

	region := d.Region()
	latestEpoch := region.GetRegionEpoch()

	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to Scheduler.
	if latestEpoch.Version != epoch.Version {
		log.Infof("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.Tag, latestEpoch, epoch)
		return &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	d.ApproximateSize = &size
}

func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)

	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
	compactedIdx := d.peerStorage.truncatedIndex()
	compactedTerm := d.peerStorage.truncatedTerm()
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			if key.Term < compactedTerm || key.Index < compactedIdx {
				log.Infof("%s snap file %s has been compacted, delete", d.Tag, key)
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				if time.Since(modTime) > 4*time.Hour {
					log.Infof("%s snap file %s has been expired, delete", d.Tag, key)
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || key.Index == compactedIdx) {
			log.Infof("%s snap file %s has been applied, delete", d.Tag, key)
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}
