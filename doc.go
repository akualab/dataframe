// Copyright 2013 AKUALAB INC. All Rights Reserved.
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/*
A package to manage R-like data frames.

DataFrame

A DataFrame is a table where columns are variables and rows are measurements.
For example:

  row    room      wifi                     acceleration
  0      KITCHEN   [-56.1, -78.9, -44.12]   1.3
  1      BATH      [-58, -71.1, -39.8]      1.8
  ...

Each column correspond to a variable. Each variable can have a different type. In this case,
room is a string, wifi is an array of numbers, and acceleration is a number. In JSON:

  {
    "description": "An indoor positioning data set.",
    "batchid": "24001-015",
    "var_names": ["room", "wifi", "acceleration"],
    "data": [
      ["KITCHEN", [-56.1, -78.9, -44.12], 1.3],
      ["BATH"   , [-58, -71.1, -39.8],    1.8]
    ]
  }

DataSet

A DataSet is a collection of DataFrame files. All files must have the same schema.
The API provides methods to iterate over the DataSet which hides teh details about files from
the end user.
*/
package dataframe
