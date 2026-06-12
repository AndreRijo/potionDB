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

//An idea (that needs to be better thought of): opportunistic GC.
//Send GC request to materializer, the partitions only do the GC if their queue of reqs is empty.
//This is definitely not perfect, as GC may take long...

import (
	fmt "fmt"
	"sync/atomic"
	"time"

	"potionDB/crdt/clocksi"
	"potionDB/shared/shared"
)

// When updates/reads are ongoing, we do only fast GC, to avoid hampering performance too much.
// In fast GC, we avoid checking large embedded CRDTs and we don't allocate new slices to reduce history slice.
// This reduces allocation needs and Go's GC pressure, thus making PotionDB's GC faster.
// Other GC steps may also be skipped during fast GC.
// A normal GC will be issued when it is detected that PotionDB is in idle mode.
type GarbageCollector struct {
	tm                    *TransactionManager
	lastCleanClk          clocksi.Timestamp
	hasAutomaticGCStarted bool
	wasLastGCFastGC       bool
	manualGcChan          chan struct{}
	manualReplyChan       chan bool
	gcTicker              *time.Ticker
	matReplyChan          chan struct{}
}

const (
	GCFreq   = 5000 * time.Millisecond //ms
	GCFreq64 = 5000
)

func InitializeGarbageCollector(tm *TransactionManager) (gc *GarbageCollector) {
	gc = &GarbageCollector{tm: tm, gcTicker: time.NewTicker(24 * time.Hour),
		manualGcChan: make(chan struct{}, 1), matReplyChan: make(chan struct{}, nGoRoutines)} //Non initialized ticker - it will be initialized by StartGCTimer()
	if !shared.IsVMDisabled && !shared.IsGCDisabled {
		go gc.cleanRoutine()
	}
	return
}

// Starts periodic GC.
func (gc *GarbageCollector) StartGCTimer() {
	gc.hasAutomaticGCStarted = true
	gc.gcTicker.Reset(GCFreq)
}

func (gc *GarbageCollector) RequestManualGC(replyChan chan bool) {
	fmt.Printf("[TM]Requesting manual GC.\n")
	gc.manualReplyChan = replyChan
	gc.manualGcChan <- struct{}{}
}

// Returns true if there was some cleaning done, false if not. GC will only happen if TM's clock has advanced since last GC.
func (gc *GarbageCollector) doClean(isManualGC bool) bool {
	//fmt.Println("[GC]It's sweeping time!")
	//var tmSliceClk []int64
	/*gc.tm.localClock.Lock()
	tmClk := gc.tm.localClock.Copy()
	//tmSliceClk = gc.tm.localClock.Copy()
	gc.tm.localClock.Unlock()*/
	tmClk := gc.tm.localClock.GetClock()

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
		//safeClk = clocksi.FromSortedSliceToClockSi(tmSliceClk)
	}
	/*if safeClk.IsEqual(gc.lastCleanClk) {
		if noCleans%5 == 0 {
			fmt.Printf("[GC]Not cleaning garbage as TM's clock has not advanced since the last round of GC.\n Current clock: %v\n", safeClk.ToSortedString())
		}
		noCleans++
	} else {*/
	//fmt.Printf("[GC]Clock to clean: %s. TM clock: %v\n", safeClk.ToSortedString(), tmSliceClk)
	isFastGC, didClkAdvance := false, safeClk.IsDifferent(gc.lastCleanClk)
	if didClkAdvance && !isManualGC { //ManualGC are never fast GC.
		isFastGC = true
	}
	if (!didClkAdvance || !gc.tm.anyUpdatesSinceGC) && !gc.wasLastGCFastGC { //Last GC wasn't fast and clock didn't advance - GC would achieve nothing. Just return.
		return false
	}
	if isManualGC {
		fmt.Printf("[GC]Manual (full) GC. Clock to clean: %s\n", safeClk.ToSortedString())
		gc.wasLastGCFastGC = false
		//fmt.Printf("[GC]Manual GC. Clock to clean: %s. TM clock: %v\n", safeClk.ToSortedString(), tmClk.ToSortedString())
	} else if isFastGC {
		fmt.Printf("[GC]Automatic (fast) GC. Clock to clean: %s\n", safeClk.ToSortedString())
		gc.wasLastGCFastGC = true
	} else {
		fmt.Printf("[GC]Automatic (full) GC. Clock to clean: %s\n", safeClk.ToSortedString())
		gc.wasLastGCFastGC = false
		//fmt.Printf("[GC]Automatic GC. Clock to clean: %s. TM clock: %v\n", safeClk.ToSortedString(), tmClk.ToSortedString())
	}
	//TM opportunistically cleans itself whenever appropriate (client buffers are clean on connection loss; replication TM buffers clean themselves whenever there's nothing in queue.)
	//Replicator detects when PotionDB is idle (i.e., when it has no txn to replicate) and cleans itself. Some other buffers are automatically clean on each replication cycle.
	//Log does not need any cleaning, and that is automatically requested by Replicator regardless.
	//So Mat is the only one that needs this. In theory, Replicator could request mat to clean itself, and use the last replication clock.
	gc.tm.anyUpdatesSinceGC = false
	gc.tm.mat.SendRequestToAllChannels(MaterializerRequest{MatRequestArgs: MatGCArgs{SafeClk: safeClk, ReplyChan: gc.matReplyChan, FastGC: isFastGC}})
	gc.lastCleanClk = safeClk.Copy() //Shouldn't really need a copy...
	for i := 0; i < int(nGoRoutines); i++ {
		<-gc.matReplyChan
	}
	return true
	//}
}

func (gc *GarbageCollector) cleanRoutine() {
	//To prevent too much GC spam we only warn about "not cleaning" every 5 cleans
	noCleans, didClean, isManualGC, gcStart, gcFinish := 0, false, false, int64(0), int64(0)
	for {
		//fmt.Println("[GC]Waiting for a GC request...")
		select { //Wait for a manual request or the ticker.
		case <-gc.manualGcChan:
			fmt.Println("[GC]Received manual GC request.")
			isManualGC = true
			if gc.hasAutomaticGCStarted {
				gc.gcTicker.Reset(GCFreq)
			}
			if len(gc.gcTicker.C) > 0 { //Small chance that both automatic and manual GC are requested at the same time.
				<-gc.gcTicker.C
			}
		case <-gc.gcTicker.C:
			//fmt.Println("[GC]Received automatic GC request.")
			isManualGC = false
			//Note: If a concurrent manual GC is requested, we will still process it afterwards, in order to give a reply.
		}
		gcStart = time.Now().UnixMilli()
		didClean = gc.doClean(isManualGC)
		gcFinish = time.Now().UnixMilli()
		if didClean {
			fmt.Printf("[GC]Finished GC. Took %d ms.\n", gcFinish-gcStart)
			noCleans = 0
		} else {
			if gc.lastCleanClk != nil {
				if !isManualGC && noCleans%5 == 0 {
					fmt.Printf("[GC]Not cleaning garbage as TM's clock has not advanced since the last GC round.\n Current clock: %v\n", gc.lastCleanClk.ToSortedString())
				} else if isManualGC {
					fmt.Printf("[GC]Manual GC call was ignored as TM's clock has not advanced since the last GC round.\n Current clock: %v\n", gc.lastCleanClk.ToSortedString())
				}
			}
			noCleans++
		}
		if isManualGC && gc.manualReplyChan != nil { //Notify caller of manual GC.
			gc.manualReplyChan <- didClean
			close(gc.manualReplyChan)
			gc.manualReplyChan = nil
		}
		if gc.hasAutomaticGCStarted && gcFinish-gcStart > 2*GCFreq64/3 {
			gc.gcTicker.Reset(GCFreq)    //Reset ticker, to ensure we wait again GCFreq.
			for len(gc.gcTicker.C) > 0 { //Clear any pending ticks.
				<-gc.gcTicker.C
			}
		}
	}
}

/*func (gc *GarbageCollector) cleanRoutine() {
	//To prevent too much GC spam we only warn about "not cleaning" every 5 cleans
	noCleans, didClean := 0, false
	for {
		//fmt.Println("[GC]Sleeping...")
		time.Sleep(GCFreq)
		didClean = gc.doClean()
		if didClean {
			noCleans = 0
		} else {
			if noCleans%5 == 0 {
				fmt.Printf("[GC]Not cleaning garbage as TM's clock has not advanced since the last round of GC.\n Current clock: %v\n", gc.lastCleanClk.ToSortedString())
			}
			noCleans++
		}
	}
}*/
