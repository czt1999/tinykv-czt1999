// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
	"sort"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here (3C).
	stores := cluster.GetStores()
	maxDown := cluster.GetMaxStoreDownTime()

	// This scheduler avoids too many regions in one store.
	// First, the Scheduler will select all suitable stores.
	// Then sort them according to their region size.
	// Then the Scheduler tries to find regions to move from the store with the biggest region size.
	var originStore *core.StoreInfo
	var region *core.RegionInfo
	sort.Sort(StoreInfosByRegionSize(stores))
	for _, store := range stores {
		// The scheduler will try to find the region most suitable for moving in the store.
		// First, it will try to select a pending region because pending may mean the disk is overloaded.
		// If there isn’t a pending region, it will try to find a follower region.
		// If it still cannot pick out one region, it will try to pick leader regions.
		// Finally, it will select out the region to move, or the Scheduler will try the next store which
		// has a smaller region size until all stores will have been tried.
		if !store.IsUp() || store.DownTime() > maxDown {
			continue
		}
		cluster.GetPendingRegionsWithLock(store.GetID(), func(rc core.RegionsContainer) {
			region = rc.RandomRegion([]byte(""), []byte(""))
		})
		if region != nil {
			originStore = store
			break
		}
		cluster.GetFollowersWithLock(store.GetID(), func(rc core.RegionsContainer) {
			region = rc.RandomRegion([]byte(""), []byte(""))
		})
		if region != nil {
			originStore = store
			break
		}
		cluster.GetLeadersWithLock(store.GetID(), func(rc core.RegionsContainer) {
			region = rc.RandomRegion([]byte(""), []byte(""))
		})
		if region != nil {
			originStore = store
			break
		}
	}

	if region == nil {
		log.Warnf("[balance region] Schedule origin region is nil")
		return nil
	}
	if len(region.GetPeers()) < cluster.GetMaxReplicas() {
		log.Warnf("[balance region] Schedule region has fewer replicas %v than max %v",
			len(region.GetPeers()), cluster.GetMaxReplicas())
		return nil
	}

	// After you pick up one region to move, the Scheduler will select a store as the target.
	// Actually, the Scheduler will select the store with the smallest region size.
	var targetStore *core.StoreInfo
	storesNotMoveTo := region.GetStoreIds()
	for i := range stores {
		store := stores[len(stores)-1-i]
		if !store.IsUp() || store.DownTime() > maxDown {
			continue
		}
		if _, ok := storesNotMoveTo[store.GetID()]; ok {
			continue
		}
		targetStore = store
		break
	}
	if targetStore == nil {
		log.Warnf("[balance region] Schedule no suitable target store; region %v", region.GetID())
		return nil
	}
	// Then the Scheduler will judge whether this movement is valuable,
	// by checking the difference between region sizes of the original store and the target store.
	// If the difference is big enough, the Scheduler should allocate a new peer on the target store and create a move peer operator.

	// We have to make sure that the difference has to be bigger than two times
	// the approximate size of the region, which ensures that after moving,
	// the target store’s region size is still smaller than the original store.
	if originStore.GetRegionSize()-targetStore.GetRegionSize() < 2*region.GetApproximateSize() {
		log.Warnf("[balance region] Schedule difference of stores is not big enough %v %v %v",
			originStore.GetRegionSize(), targetStore.GetRegionSize(), region.GetApproximateSize())
		return nil
	}
	newPeer, err := cluster.AllocPeer(targetStore.GetID())
	if err != nil {
		log.Errorf("[balance region] cluster alloc peer err %v", err)
		return nil
	}
	if newPeer == nil {
		log.Infof("[balance region] cluster alloc no new peer")
		return nil
	}
	op, err := operator.CreateMovePeerOperator("balance region", cluster, region, operator.OpBalance, originStore.GetID(), targetStore.GetID(), newPeer.GetId())
	if err != nil {
		log.Errorf("[balance region] create op err %v", err)
		return nil
	}
	log.Infof("[balance region] op : move region %v (store %v -> %v)", region.GetID(), originStore.GetID(), targetStore.GetID())
	return op
}

type StoreInfosByRegionSize []*core.StoreInfo

func (s StoreInfosByRegionSize) Len() int {
	infos := []*core.StoreInfo(s)
	return len(infos)
}

func (s StoreInfosByRegionSize) Less(i, j int) bool {
	infos := []*core.StoreInfo(s)
	return infos[i].GetRegionSize() > infos[j].GetRegionSize()
}

func (s StoreInfosByRegionSize) Swap(i, j int) {
	infos := []*core.StoreInfo(s)
	infos[i], infos[j] = infos[j], infos[i]
}
