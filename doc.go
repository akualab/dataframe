// Copyright 2013 AKUALAB INC. All Rights Reserved.
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/*
A package to manage R-like data frames.

A DataFrame is a table where columns are variables and rows are measurements.
For example:

  ID     ROOM      WIFI                     ACC_Z
  0      KITCHEN   [-56.1, -78.9, -44.12]   1.3
  1      BATH      [-58, -71.1, -39.8]      1.8
  ...

Each column correspond to a variable. Each variable can have a different type. In this case,
int, string, float slice, and float. To read a data frame using json:

  {
    "description": "An indoor positioning data set.",
    "batchid": "24001-015",
    "var_names": ["room", "wifi", "acceleration"],
    "var_types": ["string", "[]float64", "float64"],
    "data": [
      ["KITCHEN", [-56.1, -78.9, -44.12], 1.3],
      ["BATH"   , [-58, -71.1, -39.8],    1.8]
    ]
  }

All fields are optional. It is up to the application to decide what fields to read and how to
parse the values.

A DataSet is a collection of DataFrames. The package provides methods to read a list of files where
each file contains a data frame with the same schema. This can be useful to iterate over all the
data instances (rows).
*/
package dataframe
