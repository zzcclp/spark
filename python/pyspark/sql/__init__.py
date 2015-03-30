#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
public classes of Spark SQL:

    - L{SQLContext}
      Main entry point for :class:`DataFrame` and SQL functionality.
    - L{DataFrame}
      A distributed collection of data grouped into named columns.
    - L{GroupedData}
      Aggregation methods, returned by :func:`DataFrame.groupBy`.
    - L{Column}
      A column expression in a :class:`DataFrame`.
    - L{Row}
      A row of data in a :class:`DataFrame`.
    - L{HiveContext}
      Main entry point for accessing data stored in Apache Hive.
    - L{functions}
      List of built-in functions available for :class:`DataFrame`.
"""

from pyspark.sql.context import SQLContext, HiveContext
from pyspark.sql.types import Row
from pyspark.sql.dataframe import DataFrame, GroupedData, Column, SchemaRDD

__all__ = [
    'SQLContext', 'HiveContext', 'DataFrame', 'GroupedData', 'Column', 'Row',
]
