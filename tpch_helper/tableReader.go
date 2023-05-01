package tpch

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

const (
	colSeparator = '|'
)

func ReadTable(fileLoc string, nParts int, nEntries int, toRead []int8) (tableBuf [][]string) {
	fmt.Println("[TableReader]Reading tables at: ", fileLoc)
	if file, err := getFile(fileLoc); err == nil {
		defer file.Close()
		tableBuf = make([][]string, nEntries, nEntries)
		scanner := bufio.NewScanner(file)
		i := 0
		for ; scanner.Scan(); i++ {
			tableBuf[i] = processLine(scanner.Text(), nParts, toRead)
		}
	}
	return
}

func processLine(line string, nParts int, toRead []int8) (result []string) {
	result = make([]string, nParts)

	parts := strings.SplitN(line, "|", nParts)
	//remove last "|"
	last := parts[len(parts)-1]
	parts[len(parts)-1] = last[0 : len(last)-1]
	for _, partI := range toRead {
		result[partI] = parts[partI]
	}
	/*
		var curr strings.Builder
		i := 0

		for _, char := range line {
			if char != colSeparator {
				curr.WriteRune(char)
			} else {
				result[i] = curr.String()
				curr.Reset()
				i++
			}
		}
	*/

	return
}

func ReadHeaders(headerLoc string, nTables int) (headers [][]string, keys [][]int, toRead [][]int8) {
	if file, err := getFile(headerLoc); err == nil {
		defer file.Close()
		//table -> array of fieldNames
		headers = make([][]string, nTables)
		//table -> array of positions to be read
		toRead = make([][]int8, nTables)
		//For each table, contains the position in the header of the primary key components
		keys = make([][]int, nTables)
		scanner := bufio.NewScanner(file)

		nLine, nCol, nColWithIgnore := -1, -1, int8(-1)
		key := strings.Builder{}
		for scanner.Scan() {
			processHeaderLine(scanner.Text(), headers, toRead, keys, &nLine, &nCol, &nColWithIgnore, &key)
		}
	}
	return
}

func processHeaderLine(line string, headers [][]string, toRead [][]int8, keys [][]int,
	nLine *int, nCol *int, nColWithIgnore *int8, key *strings.Builder) {
	if len(line) == 0 {
		return
	}
	//*: tableName. $: total number of entries/fields. +: number of fields to be processed.
	//#: parts of primary key. -: fields to ignore
	switch line[0] {
	case '*':
		*nLine++
		keys[*nLine] = make([]int, 0, 4)
	case '$':
		nEntries, _ := strconv.Atoi(line[1:])
		*nCol, *nColWithIgnore = 0, 0
		headers[*nLine] = make([]string, nEntries)
	case '+':
		nEntries, _ := strconv.Atoi(line[1:])
		toRead[*nLine] = make([]int8, nEntries)
	case '#':
		headers[*nLine][*nCol] = line[1:]
		toRead[*nLine][*nCol] = *nColWithIgnore
		keys[*nLine] = append(keys[*nLine], *nCol)
		*nCol++
		*nColWithIgnore++
	case '-':
		*nColWithIgnore++
	default:
		headers[*nLine][*nCol] = line
		toRead[*nLine][*nCol] = *nColWithIgnore
		*nCol++
		*nColWithIgnore++
	}
	return
}

//Used for mix clients which need lineItemSizes to be per order instead of per file.
func ReadUpdatesPerOrder(fileLocs []string, nEntries []int, nParts []int, toRead [][]int8, startFile, finishFile int) (ordersUpds [][]string,
	lineItemUpds [][]string, deleteKeys []string, lineItemSizes []int, itemSizesPerOrder []int) {

	fmt.Println("[TableReader]Reading updates at: ", fileLocs)

	nFiles, startFile64 := finishFile-startFile+1, int64(startFile)
	ordersUpds, lineItemUpds, deleteKeys, lineItemSizes, itemSizesPerOrder = make([][]string, nEntries[0]*nFiles), make([][]string, nEntries[1]*nFiles),
		make([]string, nEntries[2]*nFiles), make([]int, nFiles), make([]int, nEntries[0]*nFiles)
	orderI, lineI, deleteI := 0, 0, 0
	currOrderKey, currOrderItems, previousSize, currSize, currOrderI := "", 0, 0, 0, 0
	i, j, nFiles64 := int64(0), int(0), int64(nFiles)

	for ; i < nFiles64; i, deleteI = i+1, deleteI+nEntries[2] {
		//previousOrderI := orderI
		//Read a file with orders, and the file with all of that order's items
		orderI += processUpdFile(fileLocs[0]+strconv.FormatInt(i+startFile64, 10), nEntries[0], nParts[0], toRead[0], ordersUpds[orderI:])
		//fmt.Println("Diff orderI:", orderI-previousOrderI)
		lineItemSizes[i] = processUpdFile(fileLocs[1]+strconv.FormatInt(i+startFile64, 10), nEntries[1], nParts[1], toRead[1], lineItemUpds[lineI:])
		currSize += lineItemSizes[i]
		/*
			if i == 0 {
				currOrderKey = lineItemUpds[0][0]
			}
		*/
		currOrderKey = lineItemUpds[previousSize][0]
		//Counting number of items per order
		for j = previousSize; j < currSize; j++ {
			if lineItemUpds[j][0] != currOrderKey {
				itemSizesPerOrder[currOrderI] = currOrderItems
				currOrderItems, currOrderKey = 0, lineItemUpds[j][0]
				currOrderI++
			}
			currOrderItems++
		}
		previousSize, itemSizesPerOrder[currOrderI] = currSize, currOrderItems
		currOrderItems = 0
		currOrderI++ //Go to first order of the next file

		lineI = currSize
		processDeleteFile(fileLocs[2]+strconv.FormatInt(i+startFile64, 10), nEntries[2], deleteKeys[deleteI:])
	}

	/*
		fmt.Println("LineSizes before:")
		for i, lineSize := range lineItemSizes {
			fmt.Print(lineSize, " ")
			if i > 0 && i%10 == 0 {
				fmt.Println()
			}
		}
		fmt.Println()
		fmt.Printf("Last orderkey: %s, NOrders: %d\n", currOrderKey, orderI)
	*/
	/*
		lineItemPos, previous, orderIndex := 0, 0, 0
		orderID := ""
		//Fixing lineItemSizes
		for k := 1; k <= nFiles; k++ {
			orderIndex = k*nEntries[0] - k + (k-1)*2
			if orderIndex >= orderI {
				k--
				orderI = k*nEntries[0] - k + (k-1)*2 + 1
				lineI = lineItemPos
				finishFile = k + startFile
				break
			}
			orderID = ordersUpds[k*nEntries[0]-k+(k-1)*2][0]
			//lineItemPos += int(float64(nEntries[0]) * 2)
			//Use below for 0.1SF
			lineItemPos += int(float64(nEntries[0]) * 3) //Always safe to skip this amount at least
			for ; len(lineItemUpds[lineItemPos][0]) < len(orderID) || lineItemUpds[lineItemPos][0] <= orderID; lineItemPos++ {
				//Note: We're comparing strings and not ints here, thus we must compare the len to deal with cases like 999 < 1000
			}
			lineItemSizes[k-1] = lineItemPos - previous
			previous = lineItemPos
			//fmt.Println(orderID, lineItemUpds[lineItemPos-1][0], k, lineItemSizes[k-1])
		}

		fmt.Println("LineSizes after:")
		for i, lineSize := range lineItemSizes {
			fmt.Print(lineSize, " ")
			if i > 0 && i%10 == 0 {
				fmt.Println()
			}
		}
		fmt.Println()
	*/
	fmt.Println("OrderI, LineI:", orderI, lineI)
	ordersUpds, lineItemUpds, deleteKeys, itemSizesPerOrder = ordersUpds[:orderI], lineItemUpds[:lineI], deleteKeys[:orderI], itemSizesPerOrder[:orderI]
	return
}

//File order: orders, lineitem, delete
//fileLocs and nEntries are required for the 3 files. nParts is only required for the first two.
func ReadUpdates(fileLocs []string, nEntries []int, nParts []int, toRead [][]int8, nFiles int) (ordersUpds [][]string,
	lineItemUpds [][]string, deleteKeys []string, lineItemSizes []int, newNFiles int) {

	fmt.Println("[TableReader]Reading updates at: ", fileLocs)
	ordersUpds, lineItemUpds, deleteKeys = make([][]string, nEntries[0]*nFiles), make([][]string, nEntries[1]*nFiles), make([]string, nEntries[2]*nFiles)
	lineItemSizes = make([]int, nFiles)
	orderI, lineI, deleteI, newNFiles := 0, 0, 0, nFiles
	i, nFiles64 := int64(1), int64(nFiles)
	for ; i <= nFiles64; i, deleteI = i+1, deleteI+nEntries[2] {
		orderI += processUpdFile(fileLocs[0]+strconv.FormatInt(i, 10), nEntries[0], nParts[0], toRead[0], ordersUpds[orderI:])
		lineItemSizes[i-1] = processUpdFile(fileLocs[1]+strconv.FormatInt(i, 10), nEntries[1], nParts[1], toRead[1], lineItemUpds[lineI:])
		lineI += lineItemSizes[i-1]
		processDeleteFile(fileLocs[2]+strconv.FormatInt(i, 10), nEntries[2], deleteKeys[deleteI:])
	}

	lineItemPos, previous, orderIndex := 0, 0, 0
	orderID := ""
	//Fixing lineItemSizes
	for k := 1; k <= nFiles; k++ {
		orderIndex = k*nEntries[0] - k + (k-1)*2
		if orderIndex >= orderI {
			k--
			orderI = k*nEntries[0] - k + (k-1)*2 + 1
			lineI = lineItemPos
			newNFiles = k
			break
		}
		orderID = ordersUpds[k*nEntries[0]-k+(k-1)*2][0]
		//lineItemPos += int(float64(nEntries[0]) * 2)
		//Use below for 0.1SF
		lineItemPos += int(float64(nEntries[0]) * 3) //Always safe to skip this amount at least
		for ; len(lineItemUpds[lineItemPos][0]) < len(orderID) || lineItemUpds[lineItemPos][0] <= orderID; lineItemPos++ {
			//Note: We're comparing strings and not ints here, thus we must compare the len to deal with cases like 999 < 1000
		}
		lineItemSizes[k-1] = lineItemPos - previous
		previous = lineItemPos
		//fmt.Println(orderID, lineItemUpds[lineItemPos-1][0], k, lineItemSizes[k-1])
	}
	fmt.Println("OrderI, LineI:", orderI, lineI)
	ordersUpds, lineItemUpds, deleteKeys = ordersUpds[:orderI], lineItemUpds[:lineI], deleteKeys[:orderI]
	return
}

//Lineitems has a random number of entries
func processUpdFile(fileLoc string, nEntries int, nParts int, toRead []int8, tableUpds [][]string) (linesRead int) {
	if file, err := getFile(fileLoc); err == nil {
		defer file.Close()
		scanner := bufio.NewScanner(file)
		linesRead = 0
		for ; scanner.Scan(); linesRead++ {
			tableUpds[linesRead] = processLine(scanner.Text(), nParts, toRead)
		}
	}
	return
}

func processDeleteFile(fileLoc string, nEntries int, deleteKeys []string) {
	if file, err := getFile(fileLoc); err == nil {
		defer file.Close()
		scanner := bufio.NewScanner(file)
		i := 0
		currLine := ""
		for ; scanner.Scan(); i++ {
			currLine = scanner.Text()
			//Removing the "|" at the end
			deleteKeys[i] = currLine[:len(currLine)-1]
		}
	}
}

func getFile(fileLoc string) (file *os.File, err error) {
	file, err = os.Open(fileLoc)
	if err != nil {
		fmt.Println("Failed to open file", fileLoc)
		fmt.Println(err)
	}
	return
}

func ReadOrderUpdates(baseFileName string, nEntries int, nParts int, toRead []int8, nFiles int) (ordersUpds [][]string) {
	fmt.Println("[TableReader]Reading order updates at: ", baseFileName)
	fileType := ".tbl.u"
	ordersUpds = make([][]string, nEntries*int(nFiles))
	nextStart, nFiles64 := 0, int64(nFiles)
	var i int64
	for i = 1; i <= nFiles64; i++ {
		processUpdFileWithSlice(baseFileName+fileType+strconv.FormatInt(i, 10), nEntries, nParts, toRead, ordersUpds, nextStart)
		nextStart += nEntries
	}
	return
}

func processUpdFileWithSlice(fileLoc string, nEntries int, nParts int, toRead []int8, upds [][]string, startPos int) {
	if file, err := getFile(fileLoc); err == nil {
		defer file.Close()
		scanner := bufio.NewScanner(file)
		i := startPos
		for ; scanner.Scan(); i++ {
			upds[i] = processLine(scanner.Text(), nParts, toRead)
		}
	}
}
