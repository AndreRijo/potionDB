package tpch

import (
	"fmt"
	"strconv"
)

//TODO: Migrate some of tpchClient.go variables to use these ones instead.
//Contains some basic variables. Users are recommended to clean unecessary variables to save memory
type TpchData struct {
	Tables  [][][]string //Base, unprocessed, tables
	ToRead  [][]int8     //Fields of each table that should be read
	Keys    [][]int      //Fields that compose the primary key of each table
	Headers [][]string   //The name of each field in each table

	ProcTables *Tables
	TpchConfigs
}

type TpchConfigs struct {
	Sf      float64
	DataLoc string
}

const (
	TableFormat, UpdFormat, Header                = "2.18.0_rc2/tables/%sSF/", "2.18.0_rc2/upds/%sSF/", "tpc_h/tpch_headers_min.txt"
	TableExtension, UpdExtension, DeleteExtension = ".tbl", ".tbl.u", "."
)

var (
	//Contants
	TableNames   = [...]string{"customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"}
	TableEntries = [...]int{150000, 60175, 25, 1500000, 200000, 800000, 5, 10000}
	TableParts   = [...]int{8, 16, 4, 9, 9, 5, 3, 7}
	TableUsesSF  = [...]bool{true, false, false, true, true, true, false, true}
	UpdsNames    = [...]string{"orders", "lineitem", "delete"}
	UpdEntries   = [...]int{1500, 6010, 1500}

	TableFolder, UpdFolder, HeaderLoc string
	UpdCompleteFilename               [3]string
)

func (data *TpchData) PrepareBaseData() {
	fmt.Println("[TPCH]Doing preparatory work...")
	data.PrepVars()
	data.FixTableEntries()
	fmt.Println("[TPCH]Reading base data...")
	data.ReadBaseData()
	fmt.Println("[TPCH]Processing base data...")
	data.ProcessBaseData()
}

func (data *TpchData) PrepVars() {
	scaleFactorS := strconv.FormatFloat(data.Sf, 'f', -1, 64)
	TableFolder, UpdFolder = data.DataLoc+fmt.Sprintf(TableFormat, scaleFactorS), data.DataLoc+fmt.Sprintf(UpdFormat, scaleFactorS)
	UpdCompleteFilename = [3]string{UpdFolder + UpdsNames[0] + UpdExtension, UpdFolder + UpdsNames[1] + UpdExtension,
		UpdFolder + UpdsNames[2] + DeleteExtension}
	HeaderLoc = data.DataLoc + Header
	data.ProcTables = &Tables{}
	data.ProcTables.InitConstants(false)

}

func (data *TpchData) FixTableEntries() {
	switch data.Sf {
	case 0.01:
		TableEntries[LINEITEM] = 60175
		//updEntries = []int{10, 37, 10}
		UpdEntries = [...]int{15, 41, 16}
	case 0.1:
		TableEntries[LINEITEM] = 600572
		//updEntries = []int{150, 592, 150}
		//updEntries = []int{150, 601, 150}
		UpdEntries = [...]int{151, 601, 150}
	case 0.2:
		TableEntries[LINEITEM] = 1800093
		UpdEntries = [...]int{300, 1164, 300} //NOTE: FAKE VALUES!
	case 0.3:
		TableEntries[LINEITEM] = 2999668
		UpdEntries = [...]int{450, 1747, 450} //NOTE: FAKE VALUES!
	case 1:
		TableEntries[LINEITEM] = 6001215
		//updEntries = []int{1500, 5822, 1500}
		//updEntries = []int{1500, 6001, 1500}
		UpdEntries = [...]int{1500, 6010, 1500}
	}
}

func (data *TpchData) ReadBaseData() {
	data.Headers, data.Keys, data.ToRead = ReadHeaders(HeaderLoc, len(TableNames))
	data.Tables = make([][][]string, len(TableNames))
	//Force these to be read first
	data.readTable(REGION)
	data.readTable(NATION)
	data.readTable(SUPPLIER)
	data.readTable(CUSTOMER)
	data.readTable(ORDERS)
	//Order is irrelevant now
	data.readTable(LINEITEM)
	data.readTable(PARTSUPP)
	data.readTable(PART)

	data.ProcTables.NationsByRegion = CreateNationsByRegionTable(data.ProcTables.Nations, data.ProcTables.Regions)
}

func (data *TpchData) readTable(tableN int) {
	fmt.Println("Reading", TableNames[tableN], tableN)
	nEntries := TableEntries[tableN]
	if TableUsesSF[tableN] {
		nEntries = int(float64(nEntries) * data.Sf)
	}
	data.Tables[tableN] = ReadTable(TableFolder+TableNames[tableN]+TableExtension, TableParts[tableN], nEntries, data.ToRead[tableN])
}

func (data *TpchData) ProcessBaseData() {
	data.processTable(REGION)
	data.processTable(NATION)
	data.processTable(SUPPLIER)
	data.processTable(CUSTOMER)
	data.processTable(ORDERS)
	data.processTable(LINEITEM)
	data.processTable(PARTSUPP)
	data.processTable(PART)
}

func (data *TpchData) processTable(tableN int) {
	switch tableN {
	case CUSTOMER:
		data.ProcTables.CreateCustomers(data.Tables)
	case LINEITEM:
		data.ProcTables.CreateLineitems(data.Tables)
	case NATION:
		data.ProcTables.CreateNations(data.Tables)
	case ORDERS:
		data.ProcTables.CreateOrders(data.Tables)
	case PART:
		data.ProcTables.CreateParts(data.Tables)
	case REGION:
		data.ProcTables.CreateRegions(data.Tables)
	case PARTSUPP:
		data.ProcTables.CreatePartsupps(data.Tables)
	case SUPPLIER:
		data.ProcTables.CreateSuppliers(data.Tables)
	}
}

func (data *TpchData) CleanAll() {
	data.Tables, data.ToRead, data.Keys, data.Headers, data.ProcTables = nil, nil, nil, nil, nil
}
