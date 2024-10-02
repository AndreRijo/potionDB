package components

//TODO: Possible optimization: keep a boolean in each VM stating if the object has been updated since last GC
//If it has not, no need to do GC. This can speed up and the storage/processing cost is small.
//Would make a lot of sense for situations like TPC-H where a lot of the objects are not updated.
//Problem is, this doesn't work. As the previous round of GC may be for an old version
//Or can make this work, but need to leave the bool "on" after a GC that does not clean everything
//TODO: Possibly support a way for GC to be done as objects are accessed? I.e., do GC with reads/updates
//Advantage: can clean without delaying reads/updates
//Disadvantage: extra check on read/update. Probably the cost is ignorable.
//Another disadvantage: objects that are not being accessed are not cleaned.
//Ideally would need a way to check whenever partitions are busy (clean on access) or free (clean all)
//Also, make sure we only clean each object once per GC request (i.e., doing GC every update would be too heavy.)
//This part is likely tricky. Would probably need a map to keep track of objects are GC'd
//Or some GC ID that is left on each object to know the last time they were GC'ed.
//Maybe only implement this when doing performance tests.

import (
	fmt "fmt"
	"sync/atomic"
	"time"

	"potionDB/crdt/clocksi"
	"potionDB/shared/shared"
)

type GarbageCollector struct {
	tm           *TransactionManager
	lastCleanClk clocksi.Timestamp
}

const (
	GCFreq = 5000 * time.Millisecond //ms
)

func InitializeGarbageCollector(tm *TransactionManager) (gc *GarbageCollector) {
	gc = &GarbageCollector{tm: tm}
	if !shared.IsVMDisabled && !shared.IsGCDisabled {
		go gc.cleanRoutine()
	}
	return
}

func (gc *GarbageCollector) cleanRoutine() {
	//To prevent too much GC spam we only warn about "not cleaning" every 10 cleans
	noCleans := 0
	for {
		//fmt.Println("[GC]Sleeping...")
		time.Sleep(GCFreq)
		//fmt.Println("[GC]It's sweeping time!")
		gc.tm.localClock.Lock()
		tmClk := gc.tm.localClock.Copy()
		gc.tm.localClock.Unlock()

		var safeClk clocksi.Timestamp = clocksi.HighestTs //All entries maxed
		var clk clocksi.Timestamp
		anyOngoing := false
		nEntries := atomic.LoadInt64(&gc.tm.maxIDInUse)
		//fmt.Printf("[GC]There's up to %d clients in the system\n", nEntries)
		for i := int64(0); i < nEntries; i++ {
			clk = gc.tm.clksInUse[i]
			if clk != nil && clk.IsLower(safeClk) {
				safeClk = clk
				anyOngoing = true
			}
		} //TODO: If having any problems, decrement the self replica's clock by 500ms.
		if !anyOngoing {
			//fmt.Println("[GC]No pending clock! We can clean up to the TM's clock.")
			//No pending clk.
			safeClk = tmClk
		}
		if safeClk.IsEqual(gc.lastCleanClk) {
			if noCleans%10 == 0 {
				fmt.Printf("[GC]Not cleaning garbage as TM's clock has not advanced since the last round of GC.\n Current clock: %v\n", safeClk.ToSortedString())
			}
			noCleans++
		} else {
			fmt.Printf("[GC]Clock to clean: %s. TM clock: %s\n", safeClk.ToSortedString(), tmClk.ToSortedString())
			gc.tm.mat.SendRequestToAllChannels(MaterializerRequest{MatRequestArgs: MatGCArgs{SafeClk: safeClk}})
			gc.lastCleanClk, noCleans = safeClk.Copy(), 0
		}
	}
}
