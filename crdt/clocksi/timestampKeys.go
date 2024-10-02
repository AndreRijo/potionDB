package clocksi

//This is a unelegant (I'd argue, horrible and terrible practice) way to get around
//golang's restriction of arrays' size needing to be known before hand,
//as well as the inability to use slices as map keys.
//Strings could be used but they are inneficient for "<=" comparisons
//Parsing the string would be necessary, which is costly.

//This will support up to 100 entries. For above 100, strings will be used.

/*
type TSKeyOne struct{ values [1]int }
type TSKeyTwo struct{ values [2]int }
type TSKeyThree struct{ values [3]int }
type TSKeyFour struct{ values [4]int }
type TSKeyFive struct{ values [5]int }

func (key TSKeyOne) Initialize(sliceValues []int) TSKeyOne {
	key.values[0] = sliceValues[0]
	return key
}

func (key TSKeyTwo) Initialize(sliceValues []int) TSKeyTwo {
	key.values[0], key.values[1] = sliceValues[0], sliceValues[1]
	return key
}
func (key TSKeyThree) Initialize(sliceValues []int) TSKeyTwo {
	key.values[0], key.values[1] = sliceValues[0], sliceValues[1]
	return key
}
func (key TSKeyFour) Initialize(sliceValues []int) TSKeyTwo {
	key.values[0], key.values[1] = sliceValues[0], sliceValues[1]
	return key
}
func (key TSKeyFive) Initialize(sliceValues []int) TSKeyTwo {
	key.values[0], key.values[1] = sliceValues[0], sliceValues[1]
	return key
}
*/
