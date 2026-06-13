package shared

import (
	"sync"
)

//Manages memory re-usage for queries (and possibly other entities).
//This should only be used when local reusage isn't possible
//For example, for updating, local re-usage (per client) of UpdateParameters is reasonable, as UpdateParameters is basically a pointer to the actual updates.
//This is enabled by we ensuring clients only return after partitions of Materializer have copied those to the local buffers (which is also possible as within a partition, it is single threaded)
//Thus, the slices are somewhat memory-light, even if hold by several clients.
//Furthermore, after the content is given to the partition, it is no longer needed by that client.

/*
For reading cache, a pool local to each partition may be more complicated.
After a read is executed, the result will still be used by the upper layers to convert to protobuf.
So it can't be re-used during this time. The only safe way would be to "give it back" to the partition(s) after the conversion is done.
But since protobufing is unsafe, we would need to wait until it is marshalled actually.
Furthermore, this giving back would imply synchronization (channel) with the partition, introducing more sync overhead (and thus defeating the purpose of a local cache)
Enforcing protobuf creation + marshalling during partition execution kills concurrency and slows down the partition, thus it is also unnaceptable.
Copying to another re-usable buffer is... complicated? And how many buffers would we need? Again, these re-usable buffers would need to be returned in some form.
Each client holding the buffers is a memory disaster, as some of these buffers can be dozens or hundreds of KBs in size each one (for SF=10 -> SF=100 may be even MBs)
*/

//TODO: Would be interesting a "keep-alive" option, that will keep a certain amount of buffers alive, by keeping pointers to it.
//But it's not straightforward how to do this (%-chance of adding something to keepAlive? And what if we hold pointers to things we no longer re-use?)
//But maybe the limitation above is OK, can make a pool for use-cases where ALL buffers that ever get put into the pool will eventually come again.

// Stores slices grouped by their size. Groups available are hardcoded at 100~999, 1000~9999, >10000.
// Any slice below 100 gets thrown out.
// Gets will return a slice (that may be re-used or freshly allocated) whose len matches wantedLen. Capacity may be higher.
// It is safe to call Get() for small len, as a new slice will always be freshly allocated for small len.
type SlicePool[S ~[]E, E any] struct {
	pools [3]sync.Pool
}

// Keeps a single pool of slices. Intended for when most slices are expected to have similar sizes (e.g., queries with same size result set).
type SliceSinglePool[S ~[]E, E any] struct {
	sync.Pool
	minLen int //Any Get() requesting a slice below minLen will result in a new allocation. Puts of slices below minLen will also be discarded.
}

const (
	MIN_SLICE_POOL_SIZE = 100
	POOL_SMALL          = 0
	POOL_MEDIUM         = 1
	POOL_BIG            = 2
)

// Stores objects that can be classified into "small", "medium" or "large".
// The caller is responsible for determining this.
type RelSizePool[E any] struct {
	pools [3]sync.Pool
}

// The user of this struct controls how many categories there should be.
// Note that the number of categories cannot be changed after initialization.
type CategoryPool[E any] struct {
	pools []sync.Pool
}

func MakeNewSlicePool[S ~[]E, E any]() *SlicePool[S, E] {
	return &SlicePool[S, E]{pools: [3]sync.Pool{{New: initSlice[S, E]}, {New: initSlice[S, E]}, {New: initSlice[S, E]}}}
}

func initSlice[S ~[]E, E any]() any {
	return S{}
}

func (p *SlicePool[S, E]) Get(wantedLen int) S {
	if wantedLen < MIN_SLICE_POOL_SIZE {
		return make(S, wantedLen)
	}
	pos := 0
	if wantedLen >= 10000 {
		pos = 2
	} else if wantedLen >= 1000 {
		pos = 1
	}
	toReturn := p.pools[pos].Get().(S)
	if cap(toReturn) == 0 || wantedLen > cap(toReturn) { //Nothing in pool or too small. Allocate new.
		//If there was a slice but was too small, we want to throw it out, to ensure we keep mostly the bigger ones.
		//fmt.Printf("[SlicePool]Nothing on Get or obtained is too small: got %d, requested %d. Creating %d size.\n", cap(toReturn), wantedLen, wantedLen+wantedLen/10)
		return make(S, wantedLen, wantedLen+wantedLen/10) //We add a small extra buffer, may be useful for next re-use.
	}
	//We can re-use, perfect. Slice it to wanted size.
	//fmt.Printf("[SlicePool]Found suitable slice on Get: got %d, requested %d.\n", cap(toReturn), wantedLen)
	return toReturn[:wantedLen]
}

func (p *SlicePool[S, E]) Put(slice S) {
	if cap(slice) < MIN_SLICE_POOL_SIZE { //Throw out, too small to store.
		return
	}
	if cap(slice) < 1000 {
		//fmt.Printf("[SlicePool]Putting slice of cap %d in small pool.\n", cap(slice))
		p.pools[0].Put(slice)
	} else if cap(slice) < 10000 {
		//fmt.Printf("[SlicePool]Putting slice of cap %d in medium pool.\n", cap(slice))
		p.pools[1].Put(slice)
	} else {
		//fmt.Printf("[SlicePool]Putting slice of cap %d in big pool.\n", cap(slice))
		p.pools[2].Put(slice)
	}
}

func MakeNewRelSizePool[E any]() *RelSizePool[E] {
	return &RelSizePool[E]{pools: [3]sync.Pool{{New: initObj[E]}, {New: initObj[E]}, {New: initObj[E]}}}
}

func initObj[E any]() any {
	return new(E)
}

// Pre: sizeClass is either POOL_SMALL, POOL_MEDIUM or POOL_BIG
func (p *RelSizePool[E]) Get(sizeClass int) *E {
	return p.pools[sizeClass].Get().(*E)
}

func (p *RelSizePool[E]) Put(sizeClass int, obj *E) {
	p.pools[sizeClass].Put(obj)
}

func MakeNewCategoryPool[E any](nCategories int) *CategoryPool[E] {
	pools := make([]sync.Pool, nCategories)
	for i := 0; i < nCategories; i++ {
		pools[i] = sync.Pool{New: initObj[E]}
	}
	return &CategoryPool[E]{pools: pools}
}

func (p *CategoryPool[E]) Get(category int) *E {
	if category >= 0 && category < len(p.pools) {
		return p.pools[category].Get().(*E)
	}
	return new(E)
}

func (p *CategoryPool[E]) Put(category int, obj *E) {
	if category >= 0 && category < len(p.pools) {
		p.pools[category].Put(obj)
	}
}

func MakeNewSliceSinglePool[S ~[]E, E any](minLen int) *SliceSinglePool[S, E] {
	return &SliceSinglePool[S, E]{Pool: sync.Pool{New: initSlice[S, E]}}
}

func (p *SliceSinglePool[S, E]) Get(wantedLen int) S {
	if wantedLen < p.minLen {
		return make(S, wantedLen)
	}
	toReturn := p.Pool.Get().(S)
	if cap(toReturn) == 0 || wantedLen > cap(toReturn) { //Nothing in pool or too small. Allocate new.
		//If there was a slice but was too small, we want to throw it out, to ensure we keep mostly the bigger ones.
		//fmt.Printf("[SliceSinglePool]Nothing on Get or obtained is too small: got %d, requested %d. Creating %d size.\n", cap(toReturn), wantedLen, wantedLen+wantedLen/10)
		return make(S, wantedLen, wantedLen+wantedLen/10) //We add a small extra buffer, may be useful for next re-use.
	}
	//We can re-use, perfect. Slice it to wanted size.
	//fmt.Printf("[SliceSinglePool]Found suitable slice on Get: got %d, requested %d.\n", cap(toReturn), wantedLen)
	return toReturn[:wantedLen]
}

func (p *SliceSinglePool[S, E]) Put(slice S) {
	if cap(slice) < p.minLen { //Throw out, too small to store.
		return
	}
	//fmt.Printf("[SliceSinglePool]Putting slice of cap %d in pool.\n", cap(slice))
	p.Pool.Put(slice)
}
