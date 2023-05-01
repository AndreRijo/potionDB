package tpch

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

//*****TABLES*****//

const (
	CUSTOMER, LINEITEM, NATION, ORDERS, PART, PARTSUPP, REGION, SUPPLIER = 0, 1, 2, 3, 4, 5, 6, 7
	PROMO                                                                = "PROMO"
	//O_ORDERKEY, O_CUSTKEY                                                = 0, 1
	C_NATIONKEY, L_SUPPKEY, L_ORDERKEY, N_REGIONKEY, O_CUSTKEY, PS_SUPPKEY, S_NATIONKEY, R_REGIONKEY = 3, 2, 0, 2, 1, 1, 3, 0
	O_ORDERDATE, C_MKTSEGMENT, L_SHIPDATE, L_EXTENDEDPRICE, L_DISCOUNT, O_SHIPPRIOTITY, O_ORDERKEY   = 4, 6, 10, 5, 6, 5, 0
)

//TODO: Might be wise to extend all (or at least customer/orders) with REGIONKEY for faster acessing REGIONKEY when doing queries
//Alternativelly, a map of key -> regionKey would also work just fine

//Might also be worth investigating if some of those types can't be downgraded to smaller bytesizes. Specially in lineitem

type Customer struct {
	C_CUSTKEY    int32
	C_NAME       string
	C_ADDRESS    string
	C_NATIONKEY  int8
	C_PHONE      string
	C_ACCTBAL    string
	C_MKTSEGMENT string
	C_COMMENT    string
}

type LineItem struct {
	L_ORDERKEY      int32
	L_PARTKEY       int32
	L_SUPPKEY       int32
	L_LINENUMBER    int8
	L_QUANTITY      int8
	L_EXTENDEDPRICE float64
	L_DISCOUNT      float64
	L_TAX           string
	L_RETURNFLAG    string
	L_LINESTATUS    string
	L_SHIPDATE      *Date
	L_COMMITDATE    string
	L_RECEIPTDATE   string
	L_SHIPINSTRUCT  string
	L_SHIPMODE      string
	L_COMMENT       string
}

type Nation struct {
	N_NATIONKEY int8
	N_NAME      string
	N_REGIONKEY int8
	N_COMMENT   string
}

type Orders struct {
	O_ORDERKEY      int32
	O_CUSTKEY       int32
	O_ORDERSTATUS   string
	O_TOTALPRICE    string
	O_ORDERDATE     *Date
	O_ORDERPRIORITY string
	O_CLERK         string
	O_SHIPPRIORITY  string
	O_COMMENT       string
}

type Part struct {
	P_PARTKEY     int32
	P_NAME        string
	P_MFGR        string
	P_BRAND       string
	P_TYPE        string
	P_SIZE        string
	P_CONTAINER   string
	P_RETAILPRICE string
	P_COMMENT     string
}

type Region struct {
	R_REGIONKEY int8
	R_NAME      string
	R_COMMENT   string
}

type PartSupp struct {
	PS_PARTKEY    int32
	PS_SUPPKEY    int32
	PS_AVAILQTY   int32
	PS_SUPPLYCOST float64
	PS_COMMENT    string
}

type Supplier struct {
	S_SUPPKEY   int32
	S_NAME      string
	S_ADDRESS   string
	S_NATIONKEY int8
	S_PHONE     string
	S_ACCTBAL   string
	S_COMMENT   string
}

//For Customers, Suppliers and Parts the first entry is empty in order for the ID to match the position in the array
//Orders and LineItems (on 1st position) use special positioning. Always use GetOrder() method to access them
//LineItems use double position. The first index is order (the one with special positioning), the second is the linenumber.
//PartSupps has no easy indexing for now. Nations/Regions use 0-n and so do their IDs.
type Tables struct {
	Customers         []*Customer
	LineItems         [][]*LineItem
	Nations           []*Nation
	Orders            []*Orders
	Parts             []*Part
	PartSupps         []*PartSupp
	Regions           []*Region
	Suppliers         []*Supplier
	MaxOrderLineitems int32
	//Note: Likely not all of these are needed
	Types        [][]string
	Containers   [][]string
	Segments     []string
	Priorities   []string
	Instructions []string
	Modes        []string
	Nouns        []string
	Verbs        []string
	Adjectives   []string
	Adverbs      []string
	Prepositions []string
	Auxiliaries  []string
	Terminators  []rune
	//Index helpers
	PromoParts map[int32]struct{}
	//Used by queries to know where to download the order from
	OrdersRegion []int8
	//Stores the current order to region function.
	orderToRegionFun func(int32) int8
	orderIndexFun    func(int32) int32
	//Latest added orders and lineitems. May be nil if no upds besides the initial data loading have been done
	LastAddedOrders    []*Orders
	LastAddedLineItems [][]*LineItem
	//Pos of the last order that was deleted from the original table
	LastDeletedPos int

	//For each region, stores all its nations
	NationsByRegion [][]int8

	//Variables as well since they're different depending if we're working on single-server or multi-server modes
	NationkeyToRegionkey func(int64) int8
	SuppkeyToRegionkey   func(int64) int8
	CustkeyToRegionkey   func(int64) int8
	Custkey32ToRegionkey func(int32) int8
	OrderkeyToRegionkey  func(int32) int8
	OrderToRegionkey     func(*Orders) int8
}

//*****Auxiliary data types*****//

type Date struct {
	YEAR  int16
	MONTH int8
	DAY   int8
}

//returns true if the caller is higher than the argument
func (date *Date) IsHigherOrEqual(otherDate *Date) bool {
	if date.YEAR > otherDate.YEAR {
		return true
	}
	if date.YEAR < otherDate.YEAR {
		return false
	}
	//equal years
	if date.MONTH > otherDate.MONTH {
		return true
	}
	if date.MONTH < otherDate.MONTH {
		return false
	}
	//equal year and month
	if date.DAY > otherDate.DAY {
		return true
	}
	return true
}

func (date *Date) IsSmallerOrEqual(otherDate *Date) bool {
	if date.YEAR < otherDate.YEAR {
		return true
	}
	if date.YEAR > otherDate.YEAR {
		return false
	}
	//equal years
	if date.MONTH < otherDate.MONTH {
		return true
	}
	if date.MONTH > otherDate.MONTH {
		return false
	}
	//equal year and month
	if date.DAY < otherDate.DAY {
		return false
	}
	if date.DAY > otherDate.DAY {
		return true
	}
	return true
}

func (date *Date) ToString() string {
	year, month, day := strconv.Itoa(int(date.YEAR)), strconv.Itoa(int(date.MONTH)), strconv.Itoa(int(date.DAY))
	//Add 0 if month or day <= 9
	if len(month) == 1 {
		month = "0" + month
	}
	if len(day) == 1 {
		day = "0" + day
	}
	return year + "-" + month + "-" + day
}

func (tab *Tables) GetShallowCopy() (copyTables *Tables) {
	return &Tables{
		Customers:            tab.Customers,
		LineItems:            tab.LineItems,
		Nations:              tab.Nations,
		Orders:               tab.Orders,
		Parts:                tab.Parts,
		PartSupps:            tab.PartSupps,
		Regions:              tab.Regions,
		Suppliers:            tab.Suppliers,
		MaxOrderLineitems:    tab.MaxOrderLineitems,
		Segments:             tab.Segments,
		PromoParts:           tab.PromoParts,
		orderToRegionFun:     tab.orderToRegionFun,
		orderIndexFun:        tab.orderIndexFun,
		LastDeletedPos:       tab.LastDeletedPos,
		LastAddedOrders:      tab.LastAddedOrders,
		LastAddedLineItems:   tab.LastAddedLineItems,
		NationkeyToRegionkey: tab.NationkeyToRegionkey,
		SuppkeyToRegionkey:   tab.SuppkeyToRegionkey,
		CustkeyToRegionkey:   tab.CustkeyToRegionkey,
		Custkey32ToRegionkey: tab.Custkey32ToRegionkey,
		OrderkeyToRegionkey:  tab.OrderkeyToRegionkey,
		OrderToRegionkey:     tab.OrderToRegionkey,
		NationsByRegion:      tab.NationsByRegion,
	}
}

func CreateClientTables(rawData [][][]string, singleServer bool) (tables *Tables) {
	startTime := time.Now().UnixNano() / 1000000
	lineItems, maxOrderLineitems := createLineitemTable(rawData[1], len(rawData[3]))
	parts, promoParts := createPartTable(rawData[4])
	tables = &Tables{
		Customers:         createCustomerTable(rawData[0]),
		LineItems:         lineItems,
		Nations:           createNationTable(rawData[2]),
		Orders:            createOrdersTable(rawData[3]),
		Parts:             parts,
		PartSupps:         createPartsuppTable(rawData[5]),
		Regions:           createRegionTable(rawData[6]),
		Suppliers:         createSupplierTable(rawData[7]),
		MaxOrderLineitems: maxOrderLineitems,
		Segments:          createSegmentsList(),
		PromoParts:        promoParts,
	}
	if !singleServer {
		tables.NationkeyToRegionkey = tables.nationkeyToRegionkey
		tables.SuppkeyToRegionkey = tables.suppkeyToRegionkey
		tables.CustkeyToRegionkey = tables.custkeyToRegionkey
		tables.Custkey32ToRegionkey = tables.custkey32ToRegionkey
		tables.OrderkeyToRegionkey = tables.orderkeyToRegionkey
		tables.OrderToRegionkey = tables.orderToRegionkey
	} else {
		tables.NationkeyToRegionkey = tables.singleServerToRegionkey
		tables.SuppkeyToRegionkey = tables.singleServerToRegionkey
		tables.CustkeyToRegionkey = tables.singleServerToRegionkey
		tables.Custkey32ToRegionkey = tables.singleServer32ToRegionkey
		tables.OrderkeyToRegionkey = tables.singleServer32ToRegionkey
		tables.OrderToRegionkey = tables.singleServerOrderToRegionkey
	}
	//tables.PromoParts = calculatePromoParts(tables.Parts)
	tables.orderToRegionFun = tables.orderkeyToRegionkeyMultiple
	tables.orderIndexFun = tables.getFullOrderIndex
	tables.LastDeletedPos = 1
	tables.NationsByRegion = CreateNationsByRegionTable(tables.Nations, tables.Regions)
	endTime := time.Now().UnixNano() / 1000000
	fmt.Println("Time taken to process tables:", endTime-startTime, "ms")
	return
}

func CreateNationsByRegionTable(nations []*Nation, regions []*Region) (result [][]int8) {
	result = make([][]int8, len(regions))
	//There's few nations so append is okay
	for _, nation := range nations {
		result[nation.N_REGIONKEY] = append(result[nation.N_REGIONKEY], nation.N_NATIONKEY)
	}
	return
}

func (tab *Tables) InitConstants(singleServer bool) {
	tab.Segments = createSegmentsList()
	tab.orderToRegionFun = tab.orderkeyToRegionkeyMultiple
	tab.orderIndexFun = tab.getFullOrderIndex
	tab.LastDeletedPos = 1
	if !singleServer {
		tab.NationkeyToRegionkey = tab.nationkeyToRegionkey
		tab.SuppkeyToRegionkey = tab.suppkeyToRegionkey
		tab.CustkeyToRegionkey = tab.custkeyToRegionkey
		tab.Custkey32ToRegionkey = tab.custkey32ToRegionkey
		tab.OrderkeyToRegionkey = tab.orderkeyToRegionkey
		tab.OrderToRegionkey = tab.orderToRegionkey
	} else {
		tab.NationkeyToRegionkey = tab.singleServerToRegionkey
		tab.SuppkeyToRegionkey = tab.singleServerToRegionkey
		tab.CustkeyToRegionkey = tab.singleServerToRegionkey
		tab.Custkey32ToRegionkey = tab.singleServer32ToRegionkey
		tab.OrderkeyToRegionkey = tab.singleServer32ToRegionkey
		tab.OrderToRegionkey = tab.singleServerOrderToRegionkey
	}
}

func (tab *Tables) FillOrdersToRegion(updOrders [][]string) {
	//Note: processing updOrders could be more efficient if we assume the IDs are ordered (i.e., we could avoid having to do ParseInt of orderKey))
	//Total orders is *4 the initial size, but we need to remove the "+1" existent in Orders due to the first entry being empty
	tab.OrdersRegion = make([]int8, (len(tab.Orders)-1)*4+1)
	for _, order := range tab.Orders[1:] {
		tab.OrdersRegion[order.O_ORDERKEY] = tab.Nations[tab.Customers[order.O_CUSTKEY].C_NATIONKEY].N_REGIONKEY
	}
	var custKey int64
	var orderKey int64
	for _, updOrder := range updOrders {
		custKey, _ = strconv.ParseInt(updOrder[O_CUSTKEY], 10, 32)
		orderKey, _ = strconv.ParseInt(updOrder[O_ORDERKEY], 10, 32)
		tab.OrdersRegion[orderKey] = tab.Nations[tab.Customers[custKey].C_NATIONKEY].N_REGIONKEY
	}
}

func (tab *Tables) SetOrderIndexFunToUpdates() {
	tab.orderIndexFun = tab.getUpdateOrderIndex
}

func createCustomerTable(cTable [][]string) (customers []*Customer) {
	//Customers IDs are 1 -> n, so we reserve an extra empty space at start
	//fmt.Println("Creating customer table")
	customers = make([]*Customer, len(cTable)+1)
	var nationKey int64
	for i, entry := range cTable {
		nationKey, _ = strconv.ParseInt(entry[3], 10, 8)
		customers[i+1] = &Customer{
			C_CUSTKEY:    int32(i + 1),
			C_NAME:       entry[1],
			C_ADDRESS:    entry[2],
			C_NATIONKEY:  int8(nationKey),
			C_PHONE:      entry[4],
			C_ACCTBAL:    entry[5],
			C_MKTSEGMENT: entry[6],
			C_COMMENT:    entry[7],
		}
	}
	return
}

func createLineitemTable(liTable [][]string, nOrders int) (lineItems [][]*LineItem, maxLineItem int32) {
	//fmt.Println("Creating lineItem table with size", nOrders)
	maxLineItem = 8

	lineItems = make([][]*LineItem, nOrders)
	bufItems := make([]*LineItem, maxLineItem+1)
	var newLine []*LineItem

	var partKey, orderKey, suppKey, lineNumber, quantity int64
	var convLineNumber int8
	var convOrderKey int32
	var extendedPrice, discount float64
	bufI, bufOrder := 0, 0
	tmpOrderID, _ := strconv.ParseInt(liTable[0][0], 10, 32)
	currOrderID := int32(tmpOrderID)
	for _, entry := range liTable {
		//Create lineitem
		orderKey, _ = strconv.ParseInt(entry[0], 10, 32)
		partKey, _ = strconv.ParseInt(entry[1], 10, 32)
		suppKey, _ = strconv.ParseInt(entry[2], 10, 32)
		lineNumber, _ = strconv.ParseInt(entry[3], 10, 8)
		quantity, _ = strconv.ParseInt(entry[4], 10, 8)
		convLineNumber, convOrderKey = int8(lineNumber), int32(orderKey)
		extendedPrice, _ = strconv.ParseFloat(entry[5], 32)
		discount, _ = strconv.ParseFloat(entry[6], 32)

		bufItems[bufI] = &LineItem{
			L_ORDERKEY:      convOrderKey,
			L_PARTKEY:       int32(partKey),
			L_SUPPKEY:       int32(suppKey),
			L_LINENUMBER:    convLineNumber,
			L_QUANTITY:      int8(quantity),
			L_EXTENDEDPRICE: extendedPrice,
			L_DISCOUNT:      discount,
			L_TAX:           entry[7],
			L_RETURNFLAG:    entry[8],
			L_LINESTATUS:    entry[9],
			L_SHIPDATE:      createDate(entry[10]),
			L_COMMITDATE:    entry[11],
			L_RECEIPTDATE:   entry[12],
			L_SHIPINSTRUCT:  entry[13],
			L_SHIPMODE:      entry[14],
			L_COMMENT:       entry[15],
		}

		//Check if it belongs to a new order
		if convOrderKey != currOrderID {
			//Add everything in the buffer apart from the new one to the table
			newLine = make([]*LineItem, bufI)
			for k, item := range bufItems[:bufI] {
				newLine[k] = item
			}
			lineItems[bufOrder] = newLine
			bufOrder++
			bufItems[0] = bufItems[bufI]
			currOrderID = convOrderKey
			bufI = 0
		}

		bufI++
		//fmt.Println(orderKey)
	}

	//fmt.Println("Last order for lineitemTable: ", bufItems[bufI-1])
	//fmt.Println("Last order already in table:", lineItems[bufOrder-1][len(lineItems[bufOrder-1])-1])
	//fmt.Println(currOrderID, bufOrder)
	//Last order
	//fmt.Printf("Last orderID: %s, last orderID already in lineItems: %d, trying to add: %d\n", liTable[len(liTable)-1][0], lineItems[bufOrder-1][0].L_ORDERKEY, currOrderID)
	newLine = make([]*LineItem, bufI)
	for k, item := range bufItems[:bufI] {
		newLine[k] = item
	}
	lineItems[bufOrder] = newLine
	return
}

func createNationTable(nTable [][]string) (nations []*Nation) {
	//fmt.Println("Creating nation table")
	nations = make([]*Nation, len(nTable))
	var nationKey, regionKey int64
	for i, entry := range nTable {
		nationKey, _ = strconv.ParseInt(entry[0], 10, 8)
		regionKey, _ = strconv.ParseInt(entry[2], 10, 8)
		nations[i] = &Nation{
			N_NATIONKEY: int8(nationKey),
			N_NAME:      entry[1],
			N_REGIONKEY: int8(regionKey),
			N_COMMENT:   entry[3],
		}
	}
	return
}

func createOrdersTable(oTable [][]string) (orders []*Orders) {
	//fmt.Println("Creating orders table with size", len(oTable)+1)
	orders = make([]*Orders, len(oTable)+1)
	var orderKey, customerKey int64
	for i, entry := range oTable {
		orderKey, _ = strconv.ParseInt(entry[0], 10, 32)
		customerKey, _ = strconv.ParseInt(entry[1], 10, 32)
		orders[i+1] = &Orders{
			O_ORDERKEY:      int32(orderKey),
			O_CUSTKEY:       int32(customerKey),
			O_ORDERSTATUS:   entry[2],
			O_TOTALPRICE:    entry[3],
			O_ORDERDATE:     createDate(entry[4]),
			O_ORDERPRIORITY: entry[5],
			O_CLERK:         entry[6],
			O_SHIPPRIORITY:  entry[7],
			O_COMMENT:       entry[8],
		}
	}
	if orders[1].O_ORDERKEY != 1 {
		//If it's not the initial data, hide the empty position
		orders = orders[1:]
	}
	return
}

func createPartTable(pTable [][]string) (parts []*Part, promoParts map[int32]struct{}) {
	//fmt.Println("Creating parts table")
	parts = make([]*Part, len(pTable)+1)
	var partKey int64
	for i, entry := range pTable {
		partKey, _ = strconv.ParseInt(entry[0], 10, 32)
		parts[i+1] = &Part{
			P_PARTKEY:     int32(partKey),
			P_NAME:        entry[1],
			P_MFGR:        entry[2],
			P_BRAND:       entry[3],
			P_TYPE:        entry[4],
			P_SIZE:        entry[5],
			P_CONTAINER:   entry[6],
			P_RETAILPRICE: entry[7],
			P_COMMENT:     entry[8],
		}
	}
	promoParts = calculatePromoParts(parts)
	return
}

func createPartsuppTable(psTable [][]string) (partSupps []*PartSupp) {
	//fmt.Println("Creating partsupp table")
	partSupps = make([]*PartSupp, len(psTable))
	var partKey, suppKey, availQty int64
	var supplyCost float64
	for i, entry := range psTable {
		partKey, _ = strconv.ParseInt(entry[0], 10, 32)
		suppKey, _ = strconv.ParseInt(entry[1], 10, 32)
		availQty, _ = strconv.ParseInt(entry[2], 10, 32)
		supplyCost, _ = strconv.ParseFloat(entry[3], 64)
		partSupps[i] = &PartSupp{
			PS_PARTKEY:    int32(partKey),
			PS_SUPPKEY:    int32(suppKey),
			PS_AVAILQTY:   int32(availQty),
			PS_SUPPLYCOST: supplyCost,
			PS_COMMENT:    entry[4],
		}
	}
	return
}

func createRegionTable(rTable [][]string) (regions []*Region) {
	//fmt.Println("Creating region table")
	regions = make([]*Region, len(rTable))
	var regionKey int64
	for i, entry := range rTable {
		regionKey, _ = strconv.ParseInt(entry[0], 10, 8)
		regions[i] = &Region{
			R_REGIONKEY: int8(regionKey),
			R_NAME:      entry[1],
			R_COMMENT:   entry[2],
		}
	}
	return
}

func createSupplierTable(sTable [][]string) (suppliers []*Supplier) {
	//fmt.Println("Creating supplier table")
	suppliers = make([]*Supplier, len(sTable)+1)
	var suppKey, nationKey int64
	for i, entry := range sTable {
		suppKey, _ = strconv.ParseInt(entry[0], 10, 32)
		nationKey, _ = strconv.ParseInt(entry[3], 0, 8)
		suppliers[i+1] = &Supplier{
			S_SUPPKEY:   int32(suppKey),
			S_NAME:      entry[1],
			S_ADDRESS:   entry[2],
			S_NATIONKEY: int8(nationKey),
			S_PHONE:     entry[4],
			S_ACCTBAL:   entry[5],
			S_COMMENT:   entry[6],
		}
	}
	return
}

func calculatePromoParts(parts []*Part) (inPromo map[int32]struct{}) {
	inPromo = make(map[int32]struct{})
	for _, part := range parts[1:] {
		if strings.HasPrefix(part.P_TYPE, PROMO) {
			inPromo[part.P_PARTKEY] = struct{}{}
		}
	}
	return
}

func (tab *Tables) GetOrderIndex(orderKey int32) (indexKey int32) {
	return tab.orderIndexFun(orderKey)
}

func (tab *Tables) getFullOrderIndex(orderKey int32) (indexKey int32) {
	//1 -> 7: 1 -> 7
	//9 -> 15: 1 -> 7
	//32 -> 39: 8 -> 15
	//40 -> 47: 8 -> 15
	//64 -> 71: 16 -> 23
	//72 -> 79: 16 -> 23
	return orderKey%8 + 8*(orderKey/32)
}

func (tab *Tables) getUpdateOrderIndex(orderKey int32) (indexKey int32) {
	return (orderKey%8 + 8*(orderKey/32)) % int32(len(tab.Orders))
}

/*
func GetLineitemIndex(lineitemKey int8, orderKey int32, maxLineitem int32) (indexKey int32) {
	//Same idea as in getOrderIndex but... we need to find a way to manage with multiple keys
	//Note that delete deletes a whole order so all of the lineitems of that order get deleted.
	//And new lineitems are for new orders.
	//Seems like each order may have up to 7 lineitems
	//1: 1-7
	//2: 8-14
	//3: 15-21
	//... 4: 22, 5: 29, 6: 36, 7: 43-49, 8:50-56,
	//9: 1-7
	//10: 8-14
	//..
	//32: 57-63
	//64: 113-119
	//65: 120-126
	//Offset of lineItem in an order + order offset before it loops (1-8 and 9-15 use same slots e.g)
	//+ space after loop
	//1-8 are special cases
	lineKey := int32(lineitemKey)
	if orderKey <= 8 {
		return lineKey + maxLineitem*(orderKey-1)
	}
	//return (lineKey % (maxLineitem + 1)) + ((maxLineitem * (orderKey - 1)) % (maxLineitem * 8)) + maxLineitem*8*(orderKey/32)
	return (lineKey % (maxLineitem + 1)) + ((maxLineitem * (orderKey)) % (maxLineitem * 8)) + maxLineitem*8*(orderKey/32)
}
*/

func createDate(stringDate string) (date *Date) {
	yearS, monthS, dayS := stringDate[0:4], stringDate[5:7], stringDate[8:10]
	year64, _ := strconv.ParseInt(yearS, 10, 16)
	month64, _ := strconv.ParseInt(monthS, 10, 8)
	day64, _ := strconv.ParseInt(dayS, 10, 8)
	return &Date{
		YEAR:  int16(year64),
		MONTH: int8(month64),
		DAY:   int8(day64),
	}
}

func createSegmentsList() []string {
	return []string{"AUTOMOBILE", "BUILDING", "FURNITURE", "MACHINERY", "HOUSEHOLD"}
}

func (tab *Tables) UpdateOrderLineitems(order [][]string, lineItems [][]string) {
	//Just call createOrder and createLineitem and store them
	//fmt.Println("First orderID, first lineItem orderID, lasts:", order[0][0], lineItems[0][0], order[len(order)-1][0], lineItems[len(lineItems)-1][0])
	//fmt.Println("Sizes:", len(order), len(lineItems))
	tab.LastAddedOrders = createOrdersTable(order)
	tab.LastAddedLineItems, _ = createLineitemTable(lineItems, len(order))
}

func (tab *Tables) CreateOrder(order []string) *Orders {
	orderKey, _ := strconv.ParseInt(order[0], 10, 32)
	customerKey, _ := strconv.ParseInt(order[1], 10, 32)
	return &Orders{
		O_ORDERKEY:      int32(orderKey),
		O_CUSTKEY:       int32(customerKey),
		O_ORDERSTATUS:   order[2],
		O_TOTALPRICE:    order[3],
		O_ORDERDATE:     createDate(order[4]),
		O_ORDERPRIORITY: order[5],
		O_CLERK:         order[6],
		O_SHIPPRIORITY:  order[7],
		O_COMMENT:       order[8],
	}
}

func (tab *Tables) CreateLineitemsOfOrder(items [][]string) (lineItems []*LineItem) {
	lineItems = make([]*LineItem, len(items))
	var partKey, orderKey, suppKey, lineNumber, quantity int64
	var convLineNumber int8
	var convOrderKey int32
	var extendedPrice, discount float64

	for i, item := range items {
		//Create lineitem
		orderKey, _ = strconv.ParseInt(item[0], 10, 32)
		partKey, _ = strconv.ParseInt(item[1], 10, 32)
		suppKey, _ = strconv.ParseInt(item[2], 10, 32)
		lineNumber, _ = strconv.ParseInt(item[3], 10, 8)
		quantity, _ = strconv.ParseInt(item[4], 10, 8)
		convLineNumber, convOrderKey = int8(lineNumber), int32(orderKey)
		extendedPrice, _ = strconv.ParseFloat(item[5], 32)
		discount, _ = strconv.ParseFloat(item[6], 32)

		lineItems[i] = &LineItem{
			L_ORDERKEY:      convOrderKey,
			L_PARTKEY:       int32(partKey),
			L_SUPPKEY:       int32(suppKey),
			L_LINENUMBER:    convLineNumber,
			L_QUANTITY:      int8(quantity),
			L_EXTENDEDPRICE: extendedPrice,
			L_DISCOUNT:      discount,
			L_TAX:           item[7],
			L_RETURNFLAG:    item[8],
			L_LINESTATUS:    item[9],
			L_SHIPDATE:      createDate(item[10]),
			L_COMMITDATE:    item[11],
			L_RECEIPTDATE:   item[12],
			L_SHIPINSTRUCT:  item[13],
			L_SHIPMODE:      item[14],
			L_COMMENT:       item[15],
		}
	}
	return
}

func (tab *Tables) CreateCustomers(table [][][]string) {
	tab.Customers = createCustomerTable(table[CUSTOMER])
}

func (tab *Tables) CreateLineitems(table [][][]string) {
	tab.LineItems, tab.MaxOrderLineitems = createLineitemTable(table[LINEITEM], len(tab.Orders)-1)
}

func (tab *Tables) CreateNations(table [][][]string) {
	tab.Nations = createNationTable(table[NATION])
}

func (tab *Tables) CreateOrders(table [][][]string) {
	tab.Orders = createOrdersTable(table[ORDERS])
}

func (tab *Tables) CreateParts(table [][][]string) {
	tab.Parts, tab.PromoParts = createPartTable(table[PART])
}

func (tab *Tables) CreateRegions(table [][][]string) {
	tab.Regions = createRegionTable(table[REGION])
}

func (tab *Tables) CreatePartsupps(table [][][]string) {
	tab.PartSupps = createPartsuppTable(table[PARTSUPP])
}

func (tab *Tables) CreateSuppliers(table [][][]string) {
	tab.Suppliers = createSupplierTable(table[SUPPLIER])
}

func (tab *Tables) nationkeyToRegionkey(nationKey int64) int8 {
	return tab.Nations[nationKey].N_REGIONKEY
}

func (tab *Tables) suppkeyToRegionkey(suppKey int64) int8 {
	return tab.Nations[tab.Suppliers[suppKey].S_NATIONKEY].N_REGIONKEY
}

func (tab *Tables) custkeyToRegionkey(custKey int64) int8 {
	return tab.Nations[tab.Customers[custKey].C_NATIONKEY].N_REGIONKEY
}

func (tab *Tables) custkey32ToRegionkey(custKey int32) int8 {
	return tab.Nations[tab.Customers[custKey].C_NATIONKEY].N_REGIONKEY
}

func (tab *Tables) orderkeyToRegionkey(orderKey int32) int8 {
	return tab.orderToRegionFun(orderKey)
}

func (tab *Tables) orderToRegionkey(order *Orders) int8 {
	return tab.Nations[tab.Customers[order.O_CUSTKEY].C_NATIONKEY].N_REGIONKEY
}

func (tab *Tables) singleServerToRegionkey(key int64) int8 {
	return 0
}

func (tab *Tables) singleServer32ToRegionkey(key int32) int8 {
	return 0
}

func (tab *Tables) singleServerOrderToRegionkey(order *Orders) int8 {
	return 0
}

func (tab *Tables) orderkeyToRegionkeyMultiple(orderKey int32) int8 {
	return tab.Nations[tab.Customers[tab.Orders[tab.GetOrderIndex(orderKey)].O_CUSTKEY].C_NATIONKEY].N_REGIONKEY
}

//Uses special array instead of consulting customers and nations tab.
//TODO: On places where OrderkeyToRegionkey is referred, replace with OrderkeyToRegionkeyDirect once it's ready.
//Maybe store the function to use in a variable and replace it once appropriate?
func (tab *Tables) orderkeyToRegionkeyDirect(orderKey int32) int8 {
	return tab.OrdersRegion[orderKey]
}

func (tab *Tables) GetNationIDsOfRegion(regionKey int) []int8 {
	return tab.NationsByRegion[regionKey]
}

func (item *LineItem) ToStringSlice() (slice []string) {
	slice = make([]string, 16) //LineItem has 16 entries
	slice[0] = strconv.Itoa(int(item.L_ORDERKEY))
	slice[1] = strconv.Itoa(int(item.L_PARTKEY))
	slice[2] = strconv.Itoa(int(item.L_SUPPKEY))
	slice[3] = strconv.Itoa(int(item.L_LINENUMBER))
	slice[4] = strconv.Itoa(int(item.L_QUANTITY))
	slice[5] = strconv.FormatFloat(item.L_EXTENDEDPRICE, 'f', 2, 64)
	slice[6] = strconv.FormatFloat(item.L_DISCOUNT, 'f', 2, 64)
	slice[7] = item.L_TAX
	slice[8] = item.L_RETURNFLAG
	slice[9] = item.L_LINESTATUS
	slice[10] = item.L_SHIPDATE.ToString()
	slice[11] = item.L_COMMITDATE
	slice[12] = item.L_RECEIPTDATE
	slice[13] = item.L_SHIPINSTRUCT
	slice[14] = item.L_SHIPMODE
	slice[15] = item.L_COMMENT
	return
}

func (tab *Tables) CustSliceToRegion(obj []string) int8 {
	nationKey, _ := strconv.ParseInt(obj[C_NATIONKEY], 10, 8)
	return tab.NationkeyToRegionkey(nationKey)
}

func (tab *Tables) LineitemSliceToRegion(obj []string) []int8 {
	suppKey, _ := strconv.ParseInt(obj[L_SUPPKEY], 10, 32)
	orderKey, _ := strconv.ParseInt(obj[L_ORDERKEY], 10, 32)
	r1, r2 := tab.SuppkeyToRegionkey(suppKey), tab.OrderkeyToRegionkey(int32(orderKey))
	if r1 == r2 {
		return []int8{r1}
	}
	return []int8{r1, r2}
}

func (tab *Tables) NationSliceToRegion(obj []string) int8 {
	regionKey, _ := strconv.ParseInt(obj[N_REGIONKEY], 10, 8)
	return int8(regionKey)
}

func (tab *Tables) OrdersSliceToRegion(obj []string) int8 {
	custKey, _ := strconv.ParseInt(obj[O_CUSTKEY], 10, 32)
	return tab.CustkeyToRegionkey(custKey)
}

func (tab *Tables) PartSuppSliceToRegion(obj []string) int8 {
	suppKey, _ := strconv.ParseInt(obj[PS_SUPPKEY], 10, 32)
	return tab.SuppkeyToRegionkey(suppKey)
}

func (tab *Tables) RegionSliceToRegion(obj []string) int8 {
	regionKey, _ := strconv.ParseInt(obj[R_REGIONKEY], 10, 8)
	return int8(regionKey)
}

func (tab *Tables) SupplierSliceToRegion(obj []string) int8 {
	nationKey, _ := strconv.ParseInt(obj[S_NATIONKEY], 10, 8)
	return tab.NationkeyToRegionkey(nationKey)
}
