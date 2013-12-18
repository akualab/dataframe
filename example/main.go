// Copyright 2013 AKUALAB INC. All Rights Reserved.
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Example program that uses the dataframe package.
package main

import (
	"fmt"

	"github.com/akualab/dataframe"
)

func main() {

	// Read a data frame from file.
	df, err := dataframe.ReadDataFrameFile("data/coil99-3.json")
	if err != nil {
		panic(err)
	}

	// Get the float variables as a single float64 slice.
	ch1 := df.GetChanFloat64("chemical_concentrations", "algae")

	// Print slices.
	var count int
	for v := range ch1 {
		fmt.Printf("n: %3d, values: %+v\n", count, v)
		count++
	}

	// Read list of files.
	ds, e := dataframe.ReadDataSet("dataset.yaml")
	if e != nil {
		panic(e)
	}

	// Count total number of instances on all files.
	ch2 := ds.GetChanFloat64("chemical_concentrations", "algae")
	count = 0
	for _ = range ch2 {
		count++
	}

	fmt.Printf("Total number of instances is %d.\n", count)
}
