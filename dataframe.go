// Copyright 2013 AKUALAB INC. All Rights Reserved.
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package dataframe

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"reflect"
	"strings"

	"github.com/golang/glog"
	"launchpad.net/goyaml"
)

const (
	BUFFER_SIZE = 1000
)

var InvalidVectorSpecError = errors.New("invalid vector spec")

// A list of dataframe files. Each file must have the same dataframe schema.
type DataSet struct {
	Path  string   `yaml:"path"`
	Files []string `yaml:"files"`
	index int
}

// A VectorSpec defines how to create a vector using a data frame.
// A vector is a subset of a data frame such that all elements of the vector have the same type.
// For example, to create a float64 vector using a subset of variables (columns) in a data frame,
// each variable must be a []float64 or a float64. The order is given by the order of VarNames.
type VectorSpec struct {
	Type     string   `yaml:"type"`
	VarNames []string `yaml:"var_names"`
}

// A DataFrame is a table where columns are variables and rows are measurements.
// Each row contains an instance. Each variable can have a different type.
type DataFrame struct {

	// Describes the data.
	Description string `json:"description"`

	// Identifies the batch or data. For example: a session, a file, etc.
	BatchID string `json:"batchid"`

	// Ordered list of variable names.
	VarNames []string `json:"var_names"`

	// Ordered list of variable types.
	VarTypes []string `json:"var_types"`

	// Ordered list of variables.
	Data [][]interface{} `json:"data"`

	// Can be used to store custom properties related to the data frame.
	Properties map[string]string `json:"properties"`

	// maps var name to var index for faster access.
	varMap map[string]int
}

// Reads a list of filenames from a file. See ReadDataSetReader()
func ReadDataSet(fn string) (ds *DataSet, e error) {

	f, e := os.Open(fn)
	if e != nil {
		return
	}
	ds, e = ReadDataSetReader(f)
	return
}

// Reads a list of filenames from an io.Reader.
func ReadDataSetReader(r io.Reader) (ds *DataSet, e error) {

	var b []byte
	b, e = ioutil.ReadAll(r)
	if e != nil {
		return
	}
	e = goyaml.Unmarshal(b, &ds)
	if e != nil {
		return
	}

	return
}

// Go back to the beginning of the data set.
func (ds *DataSet) Reset() {
	ds.index = 0
}

// Reads attributes from the next file in the data set.
// The error returns io.EOF when no more files are available.
func (ds *DataSet) Next() (df *DataFrame, e error) {

	if ds.index == len(ds.Files) {
		ds.index = 0
		return nil, io.EOF
	}
	sep := string(os.PathSeparator)
	glog.V(2).Infof("feature file: %s", ds.Path+sep+ds.Files[ds.index])
	fmt.Printf("feature file: %s\n", ds.Path+sep+ds.Files[ds.index])
	df, e = ReadDataFrameFile(ds.Path + sep + ds.Files[ds.index])
	if e != nil {
		return
	}
	ds.index++
	return
}

// Reads feature from file.
func ReadDataFrameFile(fn string) (df *DataFrame, e error) {

	f, e := os.Open(fn)
	if e != nil {
		return
	}
	return ReadDataFrame(f)
}

// Reads features from io.Reader.
func ReadDataFrame(r io.Reader) (df *DataFrame, e error) {

	var b []byte
	b, e = ioutil.ReadAll(r)
	if e != nil {
		return
	}
	df = &DataFrame{}
	e = json.Unmarshal(b, df)
	if e != nil {
		return nil, e
	}

	m := make(map[string]int)
	for k, v := range df.VarNames {
		m[v] = k
	}
	df.varMap = m
	return
}

// Joins float64 and []float64 variables and returns them as a []float64.
func (df *DataFrame) GetFrameFloat64(frame int, vector *VectorSpec) (floats []float64, err error) {

	if vector == nil {
		return nil, fmt.Errorf("vector is nil")
	}
	if vector.VarNames == nil {
		return nil, fmt.Errorf("VarNames field in vector is nil, must provide at least one var name.")
	}

	floats = make([]float64, 0)

	var indices []int
	indices, err = df.indices(vector)
	if err != nil {
		return
	}
	for _, v := range indices {
		value := df.Data[frame][v]
		switch i := value.(type) {
		case nil:
			return nil, fmt.Errorf("variable for index %d is nil.", v)
		case float64:
			floats = append(floats, i)
		case []interface{}:
			for _, v := range i {
				floats = append(floats, v.(float64))
			}
		default:
			return nil, fmt.Errorf("In frame %d, Vector of type %s in not supported.",
				frame, reflect.TypeOf(i).String())
		}
	}
	return
}

// Joins float64 and []float64 variables. Returns a channel of []float64 frames.
func (df *DataFrame) GetChanFloat64(vector *VectorSpec) (ch chan []float64) {

	ch = make(chan []float64, BUFFER_SIZE)
	go func() {
		// Iterate through all the rows.
		for i := 0; i < df.N(); i++ {
			sl, err := df.GetFrameFloat64(i, vector)
			if err != nil {
				glog.Fatalf("Reading float64 vector failed: %s", err)
			}
			ch <- sl
		}
		close(ch)
	}()

	return
}

// Resets data set and starts reading data. Returns a channel to be used to
// get all the frames.
func (ds *DataSet) GetChanFloat64(vector *VectorSpec) (ch chan []float64) {

	ch = make(chan []float64, BUFFER_SIZE)
	go func() {
		for {
			// Get a data frame.
			df, e := ds.Next()
			if e == io.EOF {
				close(ch)
				break
			}
			if e != nil {
				glog.Fatalf("Getting data frame failed: %s", e)
			}

			// Iterate through all the rows.
			for i := 0; i < len(df.Data); i++ {
				sl, err := df.GetFrameFloat64(i, vector)
				if err != nil {
					glog.Fatalf("Reading float64 vector failed: %s", err)
				}
				ch <- sl
			}
		}
	}()

	return
}

// Returns number of data instances (rows) in data frame.
func (df *DataFrame) N() int {

	return len(df.Data)
}

// Returns number of variables (columns) in data frame.
func (df *DataFrame) NumVariables() int {

	return len(df.Data[0])
}

// Returns the indices that correspond to a VectorSpec.
// Fails if variables in vector are not of the same type.
func (df *DataFrame) indices(vector *VectorSpec) (indices []int, err error) {

	indices = make([]int, 0)
	var idx int
	var ok bool
	var lastType string
	for _, v := range vector.VarNames {
		if idx, ok = df.varMap[v]; !ok {
			err = fmt.Errorf("There is no variable [%s] in the data frame.")
			return
		}
		if len(lastType) > 0 && lastType != elemType(df.VarTypes[idx]) {
			return nil, InvalidVectorSpecError
		}
		lastType = elemType(df.VarTypes[idx])
		indices = append(indices, idx)
	}
	return
}

// Get the element type. For example, if type is []float64, returns float64.
func elemType(t string) string {

	idx := strings.LastIndex(t, "]") + 1
	return t[idx:]
}
