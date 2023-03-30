package sort

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"testing"
)

func TestStdLibJSONUnmarshal(t *testing.T) {
	numBig := 1676432653945685122
	var val any
	v, _ := json.Marshal(numBig)

	// the stdlib unmarshaler will convert the number to float64 using strconv.ParseFloat
	err := json.Unmarshal(v, &val)
	if err != nil {
		t.Fatal(err)
	}
	// 1676432653945685248
	// 1.6764326539456852e+18
	t.Logf("expect: %v, actual: %v", numBig, val)
	t.Logf("expect: %v, actual: %v", numBig, int64(val.(float64)))

	parsed, err := strconv.ParseFloat("1676432653945685122", 64)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("expect: %v, actual: %v", numBig, parsed)
}

func TestStdLibJSONUnmarshalCustomDecoderNumberWithF64OK(t *testing.T) {
	numF64 := 167643.45685122
	var val any
	v, _ := json.Marshal(numF64)
	t.Logf("v: %s, numF64: %v", v, numF64)

	// the stdlib unmarshaler will try convert the number to Number using json.Number
	de := json.NewDecoder(bytes.NewReader(v))
	de.UseNumber()
	err := de.Decode(&val)
	if err != nil {
		t.Fatal(err)
	}
	f64, err := val.(json.Number).Float64()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("expect: %v, actual: %v", numF64, f64)
	vval, _ := json.Marshal(val)
	t.Logf("vval: %s, numF64: %v", vval, numF64)
}

func TestStdLibJSONUnmarshalCustomDecoderWithBigNumOK(t *testing.T) {
	numBig := int64(1676432653945685122)
	var val any
	v, _ := json.Marshal(numBig)
	t.Logf("v: %s", v)

	// the stdlib unmarshaler will convert the number to float64 using strconv.ParseFloat
	de := json.NewDecoder(bytes.NewReader(v))
	de.UseNumber()
	err := de.Decode(&val)
	if err != nil {
		t.Fatal(err)
	}
	i64, err := val.(json.Number).Int64()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("expect: %v, actual: %v", numBig, i64)

	if numBig != i64 {
		t.Fatalf("expect: %v, actual: %v", numBig, i64)
	}
}

func TestJsonFloatDecodeBigNum(t *testing.T) {
	numBig := int64(1676432653945685122)
	buf := fmt.Sprintf(`{ "demo": {"sort":[ %v, "21432243", "88.999", true, 3.14, 1.6764326539456851e+18, 0.99988765321, null]}}`, numBig)

	type DummyEle struct {
		Sort *SortType `json:"sort"`
	}
	type SearchResponse struct {
		Demo *DummyEle `json:"demo"`
	}

	var a SearchResponse

	// unmarshal test
	fmt.Println("========================== unmarshal test")
	err := json.Unmarshal([]byte(buf), &a)
	if err != nil {
		t.Fatal(err)
	}
	for idx, v := range a.Demo.Sort.Values() {
		fmt.Printf("sort ele %v: %#v\n", idx, v)
	}
	if a.Demo.Sort.Values()[0].(int64) != numBig {
		t.Fatalf("expect: %v, actual: %v", numBig, a.Demo.Sort.Values()[0])
	}
	if a.Demo.Sort.Values()[1].(string) != "21432243" {
		t.Fatalf("expect: %v, actual: %v", "21432243", a.Demo.Sort.Values()[1])
	}
	if a.Demo.Sort.Values()[2].(string) != "88.999" {
		t.Fatalf("expect: %v, actual: %v", "88.999", a.Demo.Sort.Values()[2])
	}
	if a.Demo.Sort.Values()[3].(bool) != true {
		t.Fatalf("expect: %v, actual: %v", true, a.Demo.Sort.Values()[3])
	}
	if a.Demo.Sort.Values()[4].(float64) != 3.14 {
		t.Fatalf("expect: %v, actual: %v", 3.14, a.Demo.Sort.Values()[4])
	}

	if a.Demo.Sort.Values()[a.Demo.Sort.Len()-1] != nil {
		t.Fatalf("expect: %v, actual: %v", nil, a.Demo.Sort.Values()[a.Demo.Sort.Len()-1])
	}

	// decoded value marshal test
	fmt.Println("========================== decoded value marshal test")
	xx, err := json.Marshal(&a)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("marshal again: %s\n", string(xx))

	// constructor from value and marshal test
	fmt.Println("========================== constructor from value and marshal test")
	var b SearchResponse
	b.Demo = &DummyEle{}
	b.Demo.Sort = &SortType{}
	b.Demo.Sort.Push(int64(1676432653945685122))
	b.Demo.Sort.Push("xxxxx@,\\xxx")
	b.Demo.Sort.Push(true)
	b.Demo.Sort.Push(3.14)
	oo, err := json.Marshal(&a)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("marshal oo: %s\n", string(oo))

	// test nested array
	fmt.Println("========================== test nested array")
	var c SearchResponse
	err = json.Unmarshal([]byte(`{ "demo": {"sort": [{"foo": "bar"}, [1,2,3], 4, "hello"] }}`), &c)
	if err != nil {
		t.Fatal(err)
	} else {
		t.Logf("nested result: %#v", *c.Demo.Sort)
	}
}
