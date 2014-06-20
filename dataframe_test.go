// Copyright 2013 AKUALAB INC. All Rights Reserved.
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package dataframe

import (
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/gonum/floats"
)

func getTempDir(t *testing.T) string {

	// Prepare dirs.
	tempDir, err := ioutil.TempDir("", "dataframe-test-")
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	os.MkdirAll(filepath.Join(tempDir, "data"), 0755)
	return tempDir
}

func createFileList(t *testing.T, tmpDir string) string {

	// Create file list yaml file.
	fn := filepath.Join(tmpDir, "filelist.yaml")
	t.Logf("File List: %s.", fn)
	err := ioutil.WriteFile(fn, []byte(filelistData), 0644)
	CheckError(t, err)
	return fn
}

func createDataFiles(t *testing.T, tmpDir string) (f1, f2 string) {

	// Create data file 1.
	f1 = filepath.Join(tmpDir, "data", "file1.json")
	t.Logf("Data File 1: %s.", f1)
	e := ioutil.WriteFile(f1, []byte(file1), 0644)
	CheckError(t, e)

	// Create data file 2.
	f2 = filepath.Join(tmpDir, "data", "file2.json")
	t.Logf("Data File 2: %s.", f2)
	e = ioutil.WriteFile(f2, []byte(file2), 0644)
	CheckError(t, e)

	return f1, f2
}

func TestDataSet(t *testing.T) {

	tmpDir := getTempDir(t)
	fn := createFileList(t, tmpDir)
	createDataFiles(t, tmpDir)

	// Read file list.
	ds, e := ReadDataSetFile(fn)
	CheckError(t, e)

	// Check DataSet content.
	t.Logf("DataSet: %+v", ds)

	if ds.Path != "data" {
		t.Fatalf("Path is [%s]. Expected \"data\".", ds.Path)
	}
	if ds.Files[0] != "file1.json" {
		t.Fatalf("Files[0] is [%s]. Expected \"file1\".", ds.Files[0])
	}
	if ds.Files[1] != "file2.json" {
		t.Fatalf("Files[1] is [%s]. Expected \"file2\".", ds.Files[1])
	}

	os.Chdir(tmpDir)
	//var n int
	for {
		features, e := ds.Next()
		if e == io.EOF {
			break
		}
		CheckError(t, e)
		t.Logf("data: \n%+v\n", features)
	}
}

func TestDimensions(t *testing.T) {

	tmpDir := getTempDir(t)
	f1, _ := createDataFiles(t, tmpDir)

	// Get a vector for a frame id in a data frame.
	df, dfe := ReadDataFrameFile(f1)
	CheckError(t, dfe)

	if df.N() != 6 {
		t.Fatalf("N must be 6, not %d.", df.N())
	}

	if df.NumVariables() != 3 {
		t.Fatalf("NumVariables must be 3, not %d.", df.NumVariables())
	}

}

func TestNext(t *testing.T) {

	tmpDir := getTempDir(t)
	f1, _ := createDataFiles(t, tmpDir)

	// Get a vector for a frame id in a data frame.
	df, dfe := ReadDataFrameFile(f1)
	CheckError(t, dfe)
	sl, sle := df.Float64Slice(1, "wifi", "acceleration")
	CheckError(t, sle)
	t.Logf("float slice for frame 1: %+v", sl)

	if !floats.Equal(sl, []float64{-41.8, -41.1, 1.4}) {
		t.Fatalf("vector %v doesn't match.", sl)
	}

}

func TestDataFrameChan(t *testing.T) {

	tmpDir := getTempDir(t)
	f1, _ := createDataFiles(t, tmpDir)

	df, dfe := ReadDataFrameFile(f1)
	CheckError(t, dfe)

	sl, sle := df.Float64Slice(1, "wifi", "acceleration")
	CheckError(t, sle)
	t.Logf("float slice for frame 1: %+v", sl)

	if !floats.Equal(sl, []float64{-41.8, -41.1, 1.4}) {
		t.Fatalf("vector %v doesn't match.", sl)
	}

	ch := df.Float64SliceChannel("wifi", "acceleration")

	var count int
	for v := range ch {
		t.Logf("k: %d, v: %+v", count, v)

		// Compare slice.
		sl, sle := df.Float64Slice(count, "wifi", "acceleration")
		CheckError(t, sle)

		if !floats.Equal(sl, v) {
			t.Fatalf("Mismatch in row %d: chan is %v, slice is %v.", count, v, sl)
		}
		count++
	}
}

func CheckError(t *testing.T, e error) {

	if e != nil {
		t.Fatal(e)
	}
}

const filelistData string = `
path: data
files:
  - file1.json
  - file2.json
`
const file1 string = `{
"description": "An indoor positioning data set.",
"batchid": "24001-015",
"var_names": ["room", "wifi", "acceleration"],
"data": [
["BED5",[-40.8,-41.2],1.3],
["BED5",[-41.8,-41.1],1.4],
["BED5",[-42.8,-40.34],1.5],
["DINING",[-42.9,-40.11],1.6],
["DINING",[-42.764,-39.98],1.7],
["DINING",[-42.209,-39.6],1.8]
]
}
`
const file2 string = `{
"description": "An indoor positioning data set.",
"batchid": "24001-016",
"var_names": ["room", "wifi", "acceleration"],
"data": [
["KITCHEN",[-20.1,-31.3],1.3],
["KITCHEN",[-21.8,-31.1],1.4],
["KITCHEN",[-22.8,-30.21],1.5],
["DINING",[-22.9,-30.99],1.6],
["DINING",[-22.22,-29.76],1.7],
["DINING",[-22.345,-29.6],1.8]
]
}
`
