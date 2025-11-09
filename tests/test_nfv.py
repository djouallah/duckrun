import sys
import os
import pandas as pd

# Add the parent directory to Python path to use local package source
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import duckrun

# Initialize connection
con = duckrun.connect("tmp/data.lakehouse/unsorted")

# Test RLE on calendar table - NFV should now analyze all files in the Delta table
print("Testing NFV calculation with Delta table (all files)...\n")
result = con.rle("duid","advanced")

# Display results with better formatting
if result is not None:
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', None)
    print(result)
