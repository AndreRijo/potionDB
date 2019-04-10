package crdt

type Unique uint64

//Standard map operations also work on this datatype
type UniqueSet map[Unique]struct{}

func makeUniqueSet() (set UniqueSet) {
	set = UniqueSet(make(map[Unique]struct{}))
	return
}

//Adds an element to the set. This hides the internal representation of the set
func (set UniqueSet) add(uniqueId Unique) {
	set[uniqueId] = struct{}{}
}

func (set UniqueSet) addAll(otherSet UniqueSet) {
	for key := range otherSet {
		set[key] = struct{}{}
	}
}

//Removes all elements in the intersection of both sets.
func (set UniqueSet) removeAllIn(sourceSet UniqueSet) {
	for key := range sourceSet {
		delete(set, key)
	}
}

//Same as removeAllIn, but also returns the set of intersected uniques
func (set UniqueSet) getAndRemoveIntersection(sourceSet UniqueSet) (intersectionSet UniqueSet) {
	intersectionSet = makeUniqueSet()
	hasKey := false
	for key := range sourceSet {
		_, hasKey = set[key]
		if hasKey {
			intersectionSet[key] = struct{}{}
			delete(set, key)
		}
	}
	return
}

func (set UniqueSet) copy() (copySet UniqueSet) {
	copySet = makeUniqueSet()
	for unique := range set {
		copySet[unique] = struct{}{}
	}
	return
}

/***** CONVERSION STUFF *****/

func ElementArrayToByteMatrix(elements []Element) (converted [][]byte) {
	converted = make([][]byte, len(elements))
	for i, value := range elements {
		converted[i] = []byte(value)
	}
	return
}

func ByteMatrixToElementArray(bytes [][]byte) (elements []Element) {
	elements = make([]Element, len(bytes))
	for i, value := range bytes {
		elements[i] = Element(value)
	}
	return
}

func UInt64ArrayToUniqueSet(uniques []uint64) (uniqueSet UniqueSet) {
	uniqueSet = makeUniqueSet()
	for _, unique := range uniques {
		uniqueSet.add(Unique(unique))
	}
	return
}

func UniqueSetToUInt64Array(uniqueSet UniqueSet) (uniques []uint64) {
	uniques = make([]uint64, len(uniqueSet))
	j := 0
	for unique := range uniqueSet {
		uniques[j] = uint64(unique)
		j++
	}
	return
}
