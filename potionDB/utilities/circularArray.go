package utilities

// Let's simplify... if the array gets full, a new one with double size is created.
// New elements are added to this one until the first one clears. If needed, with append.
// Once the first one clears, buf is replaced with this new one, which now starts operating as circular.
type CircularArray struct {
	buf                            []interface{}
	startPos, endPos, length, size int
	writingLinear                  bool
	linearBuf                      []interface{} //This one is non-circular, and can be expanded with append.
	linearPos                      int
}

//TODO: Delete and Write can be called from multiple threads concurrently!!! Need to fix this
//Maybe simply do a channel with size.

func (ca *CircularArray) Initialize(length int) {
	ca.buf, ca.startPos, ca.endPos, ca.length, ca.size, ca.writingLinear = make([]interface{}, length), 0, 0, length, 0, false
}

//TODO: Handle case in which array is full

func (ca *CircularArray) Write(data interface{}) (position int) {
	if ca.size == ca.length {
		ca.writingLinear, ca.linearBuf = true, make([]interface{}, 0, 2*ca.length)
	}
	if ca.writingLinear {
		ca.linearBuf, ca.linearPos = append(ca.linearBuf, data), ca.linearPos+1
		return ca.linearPos
	}
	ca.buf[ca.endPos], position, ca.size = data, ca.endPos, ca.size+1
	ca.endPos = (ca.endPos + 1) % (ca.size - 1)
	return
}

func (ca *CircularArray) Read(position int) interface{} {
	if position < ca.length {
		return ca.buf[position]
	}
	return ca.linearBuf[position-ca.length]
}

// Note: the position must not have been deleted previously.
func (ca *CircularArray) Delete(position int) {
	if position < ca.length {
		ca.buf[position] = nil
		if position == ca.endPos {
			ca.findEndPos()
		} else if position == ca.startPos {
			ca.findStartPos()
			ca.startPos++
			if ca.startPos == -1 {
				ca.startPos = ca.length - 1
			}
			if ca.endPos == ca.startPos && ca.writingLinear {
				ca.swapBuffers()
			}
		}
	} else {
		ca.linearBuf[position-ca.length] = nil
	}
}

func (ca *CircularArray) findEndPos() {
	for ; ca.buf[ca.endPos] == nil; ca.endPos-- {
		ca.size--
		if ca.endPos == -1 {
			ca.endPos = ca.length - 1
		}
		if ca.endPos == ca.startPos {
			//Buffer's empty
			if ca.writingLinear {
				ca.swapBuffers()
			}
			break
		}
	}
}

func (ca *CircularArray) findStartPos() {
	for ; ca.buf[ca.startPos] == nil; ca.startPos = (ca.startPos + 1) % ca.length {
		ca.size--
		if ca.endPos == ca.startPos {
			//Buffer's empty
			if ca.writingLinear {
				ca.swapBuffers()
			}
			break
		}
	}
}

func (ca *CircularArray) swapBuffers() {
	ca.startPos, ca.endPos = 0, ca.linearPos
	ca.buf = ca.linearBuf
	ca.linearBuf, ca.linearPos, ca.writingLinear = nil, 0, false
}

func (ca *CircularArray) ReadFirst() interface{} {
	return ca.buf[ca.startPos]
}

func (ca *CircularArray) ReadLast() interface{} {
	if ca.writingLinear {
		return ca.linearBuf[ca.linearPos-1]
	}
	return ca.buf[ca.endPos-1]
}
