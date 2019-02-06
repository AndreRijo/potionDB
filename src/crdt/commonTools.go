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

//Returns the set
func (set uniqueSet) removeAllIn(sourceSet uniqueSet) {
	for key := range sourceSet {
		delete(set, key)
	}
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
