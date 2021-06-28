package antidote

import (
	fmt "fmt"
	"potionDB/src/proto"
	"regexp"
)

//TODO: WIP
//Note: The current implementation for passing triggers between replicas creates a encoder/decoder for each trigger.
//This way, using gob isn't a problem, however it might have undesired overhead if we intend to create multiple triggers mid-benchmark.

const (
	DEFAULT_MAPPING_SIZE = 10
	DEFAULT_GENERIC_SIZE = 50

	INC, DEC           = 1, 2
	SET_ADD, SET_RMV   = 3, 4
	LWW_ADD            = 5
	TOPK_ADD, TOPK_RMV = 11, 12
	//ADD: avg, lww, set, maxmin? I don't think there's any other worth it atm (map is tough)
	AVG_ADD, AVG_MULTIPLE_ADD = 13, 14
	MAX_ADD, MIN_ADD          = 15, 16
	TOPSUM_ADD, TOPSUM_SUB    = 17, 18
)

type TriggerDB struct {
	Mapping             map[KeyParams][]AutoUpdate
	GenericMapping      map[MatchableKeyParams][]AutoUpdate
	FilteredGeneric     map[SearchParams]map[MatchableKeyParams]struct{}
	nMappings, nGeneric *int //TODO: Keep track
}

//Operation types
type OpType int

type MatchableKeyParams struct {
	Key      RegOrString
	Bucket   RegOrString
	CrdtType proto.CRDTType
}

type SearchParams struct {
	proto.CRDTType
	OpType
}

type AutoUpdate struct {
	Trigger Link
	Target  Link
}

type Link struct {
	KeyParams
	OpType
	Arguments []interface{}
}

type Variable struct {
	Name string
}

//Implemented by *Regexp (library) and MString
type RegOrString interface {
	MatchString(s string) bool
}

func (m MatchableKeyParams) Match(keyP KeyParams) bool {
	return m.CrdtType == keyP.CrdtType && m.Key.MatchString(keyP.Key) && m.Bucket.MatchString(keyP.Bucket)
}

type MString string

func (m MString) MatchString(s string) bool {
	return string(m) == s
}

func (db TriggerDB) getNTriggers() int {
	return *db.nMappings
}

func (db TriggerDB) getNGenericTriggers() int {
	return *db.nGeneric
}

func InitializeTriggerDB() TriggerDB {
	return TriggerDB{Mapping: make(map[KeyParams][]AutoUpdate), GenericMapping: make(map[MatchableKeyParams][]AutoUpdate),
		FilteredGeneric: make(map[SearchParams]map[MatchableKeyParams]struct{}), nMappings: new(int), nGeneric: new(int)}
}

func (db TriggerDB) GetMatchableKeyParams(key, bucket string, crdtType proto.CRDTType) MatchableKeyParams {
	return MatchableKeyParams{Key: db.stringToRegOrString(key), Bucket: db.stringToRegOrString(bucket), CrdtType: crdtType}
}

func (db TriggerDB) stringToRegOrString(s string) RegOrString {
	for _, c := range s {
		if c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' {
			continue //normal string
		} else {
			exp, err := regexp.Compile(s)
			fmt.Printf("Generated reg exp: %v. Error: %v\n", exp, err)
			return exp
		}
	}
	return MString(s)
}

func (db TriggerDB) AddObject(params KeyParams) {
	if _, has := db.Mapping[params]; !has {
		db.Mapping[params] = make([]AutoUpdate, 0, DEFAULT_MAPPING_SIZE)
	}
}

func (db TriggerDB) AddLink(first, second Link) AutoUpdate {
	autoUpd := AutoUpdate{Trigger: first, Target: second}
	db.Mapping[first.KeyParams] = append(db.Mapping[first.KeyParams], autoUpd)
	*db.nMappings++
	fmt.Printf("Added non-generic trigger: %v\n", autoUpd)
	return autoUpd
}

func (db TriggerDB) AddGenericLink(matchKey MatchableKeyParams, first, second Link) AutoUpdate {
	autoUpd := AutoUpdate{Trigger: first, Target: second}
	db.GenericMapping[matchKey] = append(db.GenericMapping[matchKey], autoUpd)
	filter := SearchParams{CRDTType: matchKey.CrdtType, OpType: first.OpType}
	filterKeys, has := db.FilteredGeneric[filter]
	if !has {
		filterKeys = make(map[MatchableKeyParams]struct{})
		db.FilteredGeneric[filter] = filterKeys
	}
	filterKeys[matchKey] = struct{}{}
	*db.nGeneric++
	fmt.Printf("Added generic trigger: %v\n", autoUpd)
	return autoUpd
}

func (db TriggerDB) DebugPrint(prefix string) {
	fmt.Println()
	fmt.Printf("%sTRIGGER_DB DEBUG PRINT\n", prefix)
	fmt.Printf("%sNon-generic mapping: %v\n", prefix, db.Mapping)
	fmt.Printf("%sGeneric mapping: %v\n", prefix, db.GenericMapping)
	fmt.Printf("%sNon-generic/generic counting: %d/%d\n", prefix, db.nMappings, db.nGeneric)
	fmt.Printf("%sFiltered mapping: %v\n", prefix, db.FilteredGeneric)
	fmt.Println()
}
