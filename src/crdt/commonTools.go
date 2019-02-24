package crdt

type unique uint64

//Standard map operations also work on this datatype
type uniqueSet map[unique]struct{}

func makeUniqueSet() (set uniqueSet) {
	set = uniqueSet(make(map[unique]struct{}))
	return
}

//Adds an element to the set. This hides the internal representation of the set
func (set uniqueSet) add(uniqueId unique) {
	set[uniqueId] = struct{}{}
}

func (set uniqueSet) addAll(otherSet uniqueSet) {
	for key := range otherSet {
		set[key] = struct{}{}
	}
}

//Removes all elements in the intersection of both sets.
func (set uniqueSet) removeAllIn(sourceSet uniqueSet) {
	for key := range sourceSet {
		delete(set, key)
	}
}

//Same as removeAllIn, but also returns the set of intersected uniques
func (set uniqueSet) getAndRemoveIntersection(sourceSet uniqueSet) (intersectionSet uniqueSet) {
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

func (set uniqueSet) copy() (copySet uniqueSet) {
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
