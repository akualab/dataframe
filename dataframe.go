// Copyright 2013 AKUALAB INC. All Rights Reserved.
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package dataframe

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"reflect"

	"github.com/golang/glog"
	"launchpad.net/goyaml"
)

const (
	BUFFER_SIZE = 1000
)

// A list of dataframe files. Each file must have the same dataframe schema.
type DataSet struct {
	Path  string   `yaml:"path"`
	Files []string `yaml:"files"`
	index int
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

	// Ordered list of variables.
	Data [][]interface{} `json:"data"`

	// Can be used to store custom properties related to the data frame.
	Properties map[string]string `json:"properties"`

	// maps var name to var index for faster access.
	varMap map[string]int
}

// Reads a list of filenames from a file. See ReadDataSetReader()
func ReadDataSetFile(fn string) (ds *DataSet, e error) {

	f, e := os.Open(fn)
	if e != nil {
		return
	}
	ds, e = ReadDataSet(f)
	return
}

// Reads a list of filenames from an io.Reader.
func ReadDataSet(r io.Reader) (ds *DataSet, e error) {

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
func (df *DataFrame) Float64Slice(frame int, names ...string) (floats []float64, err error) {

	if len(names) == 0 {
		return nil, fmt.Errorf("No variable names were specified, must provide at least one var name.")
	}

	floats = make([]float64, 0)

	var indices []int
	indices, err = df.indices(names...)
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
func (df *DataFrame) Float64SliceChannel(names ...string) (ch chan []float64) {

	ch = make(chan []float64, BUFFER_SIZE)
	go func() {
		// Iterate through all the rows.
		for i := 0; i < df.N(); i++ {
			sl, err := df.Float64Slice(i, names...)
			if err != nil {
				glog.Fatalf("Reading float64 vector failed: %s", err)
			}
			ch <- sl
		}
		close(ch)
	}()

	return
}

// Returns value of a string variable.
func (df *DataFrame) String(frame int, name string) (value string, err error) {

	var indices []int
	indices, err = df.indices(name)
	if err != nil {
		return
	}
	if len(indices) == 0 {
		err = fmt.Errorf("Failed to find a variable with name: [%s]", name)
		return
	}

	var ok bool
	v := df.Data[frame][indices[0]]
	value, ok = v.(string)
	if ok {
		return
	}

	err = fmt.Errorf("In frame %d, variable [%d] is of type [%s]. Must be of type string.",
		frame, name, reflect.TypeOf(v).String())
	return
}

// Resets data set and starts reading data. Returns a channel to be used to
// get all the frames.
func (ds *DataSet) Float64SliceChannel(names ...string) (ch chan []float64) {

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
				sl, err := df.Float64Slice(i, names...)
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

// Returns the indices for the variable names.
func (df *DataFrame) indices(names ...string) (indices []int, err error) {

	indices = make([]int, 0)
	var idx int
	var ok bool
	for _, v := range names {
		if idx, ok = df.varMap[v]; !ok {
			err = fmt.Errorf("There is no variable [%s] in the data frame.")
			return
		}
		indices = append(indices, idx)
	}
	return
}
