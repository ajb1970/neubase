# -*- coding: utf-8 -*-
"""
Created on Fri Dec 13 09:57:03 2019

@author: awby
"""

from sqlite3 import connect
from os import listdir, path
from pandas import DataFrame, Index, ExcelWriter, read_csv, read_excel, read_sql
from re import sub
from json import dumps, loads
from datetime import datetime
from tarfile import open as tarfile_open

na_clean=['-','*','..','.','SUPP','NA','NP','NE','NaN','DNS']

class NEUBase():
  """NEUBase database connection

  NEUBase table class includes meta data for table and columns.
  Data is stored in an SQLite database.
  """

  def __init__(self, file_location, name=None, meta=None):
    """Initialise NEUBase.

    Args:
      file_location (str): location of SQLite file
      create_db (boolean): create a new database, default is False
      meta (dictionary): creates a table 'meta' in the NEUBase database with two string variables - 'key' and 'value'
    """
    self.file_location = file_location

    if name is None:
      self.name = path.basename(file_location).split('.')[-2]

    self.list_tables()

    if not meta is None:

      if 'meta' in self.table_list:
        raise ValueError('Meta table already exists')
      else:
        keys = list(meta.keys())
        values = [ meta[key] for key in keys ]
        meta_df = DataFrame( {'values':values}, index=Index( keys ))
        self.connect()
        meta_df.to_sql( "meta", self.connection )
        self.close()
        self.meta = meta_df

    if 'meta' in self.table_list:
      self.connect()
      self.meta = read_sql( "SELECT * FROM meta", self.connection, index_col='key' ).to_dict()['value']
      self.close()
      self.meta = meta_df


  def connect(self):
    """Method to connect to SQLite database

    Creates connection accessed through connection variable.
    """
    self.connection = connect( self.file_location )
    self.cursor = self.connection.cursor()


  def close(self):
    """Closes connection to database
    """
    self.connection.close()


  def list_tables(self):
    """Lists database tables

    The list is stored in self.table_list
    Returns:
      list of table_list
    """
    self.connect()
    self.table_list_full = [ table[0] for table in self.cursor.execute("SELECT name FROM sqlite_master WHERE type ='table' AND name NOT LIKE 'sqlite_%';").fetchall() ]
    self.close()

    self.connect()
    self.view_list = [ table[0] for table in self.cursor.execute("SELECT name FROM sqlite_master WHERE type ='view' AND name NOT LIKE 'sqlite_%';").fetchall() ]
    self.close()

    self.table_list = [ table for table in self.table_list_full if not( table[-5:] == '_meta' or table[-8:] == '_columns' or table == 'meta' ) ]

    return self.table_list


  def query(self, sql, index_col=None):
    self.connect()

    if not index_col is None:
      data = read_sql( sql, self.connection, index_col = index_col )

    else:
      data = read_sql( sql, self.connection )

    self.close()
    return data


  def list_columns( self, table ):
    self.connect()
    column_list = [ col[1] for col in self.cursor.execute(f"PRAGMA table_info('{table}');").fetchall() ]
    self.close()
    return column_list


class NEUTable():
  """A SQLite table wrapper

    Args:
      meta_file: file location of the tables meta file
      data: a Pandas DataFrame holding the data from which to build the table, meta tables and columns table

    meta_file(Excel spreadsheet):
      Consists of 2 sheets - Meta and Columns

      Meta:
        2 columns key and value
        file: file location of spreadsheet (reqd)
        source: source of spreadsheet
        url: URL of spreadsheet
        sheet_name: for an Excel file, name or number of sheet
        index_col(int): column number of index (reqd)
        skiprows(int or int list): rows to skip when import database
        usecols(int list): columns to import from spreadsheet,
        names(str list): list of names to rename column headings with
        dtype(dict): dictionary with dtypes referenced by column numbers or column names
        encoding(str): encoding if importing CSV file, defaults to UTF-8

      Columns:
        input_name(str): name from spreadsheet
        db_name(str): name for SQLite database (reqd)
        an_name(str): name for ActionNetwork
        mc_name(str): name for MailChimp
        mc_col_num(int): column number for MailChimp
        output_name(str): name for Excel output
        output_width(int): column width for Excel output
        dtype(str): data type for Excel export - str, int, gbp, percent

    Output:
      self.meta: dictionary holds the tables meta data
      self.columns: a dataframe with holding columns meta data"""

  def __init__(self, name, neubase=None, data=None, meta_file=None):
    self.name = name
    self.meta_file=meta_file
    self.neubase = neubase
    self.data = data

    if not data is None:
      self.column_names_group = 'input_name'

    if neubase is None:

      if meta_file is None:
        raise ValueError("NEUBase not defined.")

    if not meta_file is None:
      self.read_meta_file( meta_file )
      self.neubase = NEUBase( self.meta['db_file'] )

    if name in self.neubase.list_tables():
      self.read_meta_tables()


  def create_table(self, meta_file=None):

    if self.name in self.neubase.list_tables():
      raise ValueError( f"{self.name} already exists.")

    if meta_file is None and self.meta_file is None:
      self.make_meta_from_data()

    if self.data is None:
      self.data = self.read_data_from_file()

    self.rename_data_column_names('db_name' )
    self.update_meta_file()
    self.update_meta_tables()
    self.overwrite_data_table()


  def update_data_value(self, column, value, where):
    sql = f"""
        UPDATE {self.name}
        SET {dumps(column)} = {dumps(value)}
        WHERE {where};
        """

    self.neubase.connect()
    self.neubase.cursor.execute( sql )
    self.neubase.connection.commit()
    self.neubase.close()


  def update_data_values(self, columns, values, where):
    sql = f"""
        UPDATE {self.name}
        SET {" = ?, ".join(columns)+" = ?"}
        WHERE {where}
        """

    self.neubase.connect()
    self.neubase.cursor.execute( sql, tuple(values) )
    self.neubase.connection.commit()
    self.neubase.close()


  def make_meta_from_data(self):
    self.meta_file = f'data/{self.name}_meta.xlsx'
    self.make_columns_meta()
    sql_index = list(self.data.index.names)
    name_map = {v: k for k, v in self.columns['input_name'].to_dict().items()}
    sql_index  = [ name_map[ i ] for i in sql_index ]

    if not 'meta' in self.__dict__.keys():
      self.meta = {}

    self.meta['name']=self.name
    self.meta['db_file']=self.neubase.file_location
    self.meta['meta_file']=self.meta_file
    self.meta['sql_index']=sql_index


  def read_meta_file(self, meta_file=None):

    if meta_file is None:

      if 'meta_file' in self.meta.keys():
        meta_file = self.meta['meta_file']

      else:
        meta_file = self.meta_file

    self.meta = read_excel( meta_file, sheet_name='Meta', index_col=0 ).to_dict()['value']
    self.convert_meta_values_from_json()
    self.columns = read_excel( meta_file, sheet_name='Columns' )
    self.columns.set_index('db_name', inplace=True)


  def read_meta_tables(self):
    self.neubase.connect()
    self.meta = read_sql( f'SELECT * FROM "{self.name}_meta"', self.neubase.connection, index_col='key' ).to_dict()['value']
    self.convert_meta_values_from_json()
    self.columns = read_sql( f'SELECT * FROM "{self.name}_columns"', self.neubase.connection, index_col='db_name' )
    self.neubase.close()


  def read_data_from_file(self):
    options = {}

    for option in [ column for column in ['skiprows','usecols','names','sheet_name', 'index_col', 'dtypes' ] if column in self.meta.keys() ]:

      if not self.meta[ option ] is None:
        options[ option ] = self.meta[ option ]

    if 'columns' in self.__dict__.keys():
      dtypes = self.columns[['input_name','dtype']].set_index('input_name').to_dict()['dtype']

    elif 'dtypes' in self.meta.keys():
      dtypes = self.meta['dtypes']

    else:
      dtypes = {}

    if self.meta['file'][-4:].lower() == '.csv':
      options[ 'dtype' ] = dtypes
      self.data = read_csv(self.meta['file'], **options )

    else:

      if 'index_col' in options.keys():
        index_col= options['index_col']
        del(options['index_col'])

      else:
        index_col= None

      self.data = read_excel(self.meta['file'], **options )

      for key in [ key for key in dtypes.keys() if key in self.data.columns]:
        self.data[key] = self.data[key].astype( dtypes[key], errors='ignore')

      if not index_col is None:
        self.data.set_index( self.data.columns[index_col], inplace=True )

    self.column_names_group = 'input_name'


  def read_data_table(self):
    self.neubase.connect()
    self.data = read_sql( f'SELECT * FROM "{self.name}"', self.neubase.connection, index_col=self.meta['sql_index'] )
    self.neubase.close()
    self.column_names_group = 'db_name'


  def list_columns(self):
    self.column_list = self.neubase.list_columns(self.name)
    return self.column_list


  def query(self, sql, index_col=None):

    if index_col is None:
      index_col = self.meta['sql_index']

    data = self.neubase.query( sql, index_col=index_col )
    return data


  def test_data_meta_match(self):
    return sorted(self.data.columns.tolist() + list(self.data.index.names)) == sorted(self.columns.index.tolist())


  def overwrite_data_table(self):

    if self.column_names_group != 'db_name':
      raise ValueError(f"Data columns are from '{self.column_names_group}' not 'db_name'.")

    if not(self.test_data_meta_match()):
      raise ValueError(f"The data columns and column meta do not match.")

    if f'{self.name}' in self.neubase.list_tables():
      self.delete_data_table()

    self.neubase.connect()
    self.data.to_sql( self.name, self.neubase.connection )
    self.neubase.close()
    self.neubase.list_tables()


  def delete_data_table(self):
    self.neubase.connect()
    self.neubase.cursor.execute( f'DROP table "{self.name}";' )
    self.neubase.connection.commit()
    self.neubase.close()


  def delete_meta_tables(self):
    self.neubase.connect()
    self.neubase.cursor.execute( f'DROP table "{self.name}_meta";' )
    self.neubase.cursor.execute( f'DROP table "{self.name}_columns";' )
    self.neubase.connection.commit()
    self.neubase.close()


  def delete_rows_from_data_table(self, where):
    """Method will delete rows from the data table.

    params:
      where: (str) an SQL WHERE statement to identify the rows to delete
        if where = 'all': all rows are deleted
    """
    if where == 'all':
      where = '1=1'
    self.neubase.connect()
    self.neubase.cursor.execute( f'DELETE FROM "{self.name}" WHERE {where};' )
    self.neubase.connection.commit()
    self.neubase.close()


  def insert_data_rows(self, columns, values):
    """Method: inserts values into the table for the columns

    params:
      columns: (list) of column headings
      values: (list) of list of values to insert
    """
    col_str = '"' + '", "'.join(columns) + '"'
    val_str = (" ?,"*len(values[0]))[:-1]
    val_data = [ tuple(vs) for vs in values ]

    sql = f"""
        INSERT INTO {self.name}({ col_str })
        VALUES({ val_str })
        """

    self.neubase.connect()
    self.neubase.cursor.executemany(sql, val_data)
    self.neubase.connection.commit()
    self.neubase.close()


  def insert_data_row(self, columns, values):
    """Method: inserts one row of values into the table for the columns

    params:
      columns: (list) of column headings
      values: (list) of values to insert
    """
    self.insert_data_rows(columns, [values])


  def update_meta_tables(self):
    """Method: updates the meta and columns tables using the current values from the class
    """
    self.neubase.connect()

    for table in ['meta','columns']:

      if f'{self.name}_{table}' in self.neubase.table_list_full:
        sql = f'DROP table "{self.name}_{table}";'
        self.neubase.cursor.execute( sql )
        self.neubase.connection.commit()

    self.make_meta_df().to_sql( f"{self.name}_meta", self.neubase.connection )
    self.columns.to_sql( f"{self.name}_columns", self.neubase.connection )
    self.neubase.close()
    self.neubase.list_tables()


  def update_meta_file(self, meta_file=None):
    """Method: replaces the meta file with a new one created from the current meta and columns data of the class

    params:
      meta_file: optional (string) file location to save the meta file
        default value: meta_file in meta
    """
    if meta_file is None:

      if 'meta_file' in self.meta.keys():
        meta_file = self.meta['meta_file']

      else:
        meta_file = self.meta_file

    writer = ExcelWriter( meta_file, engine='xlsxwriter' )
    meta_df = self.make_meta_df()
    meta_df.to_excel( writer, sheet_name='Meta')
    self.columns.to_excel( writer, sheet_name='Columns')
    writer.save()

  def rename_data_column_names(self, new_column_names_group='db_name'):
    """Method: rename the column headings of DataFrame (self.data) holding the table data be renamed using values from columns.

    params:
      new_column_names_group: (string) with the new column heading
        default: 'db_name'
    """

    if new_column_names_group == self.column_names_group:
      print(f'Column names group unchanged, already {new_column_names_group}.')
      return

    name_map = {}
    old_column_names_group_list= []
    columns = self.columns.reset_index()

    for i, row in columns.iterrows():
      name_map[ row[ self.column_names_group ] ] = row[ new_column_names_group ]
      old_column_names_group_list.append(row[ self.column_names_group ] )

    self.data.rename( columns = name_map, inplace=True )
    new_index_names = []

    for i in self.data.index.names:

      if i in old_column_names_group_list:
        new_index_names.append(name_map[i])

      else:
        new_index_names.append(i)

    self.data.index.names = new_index_names
    self.column_names_group = new_column_names_group


  def make_meta_df(self):
    """Method: Generates and returns a DataFrame with the values from self.meta
    """
    meta_data = self.convert_meta_values_to_json()
    meta_keys = list(meta_data.keys())
    meta_values = [ meta_data[key] for key in meta_keys ]

    return DataFrame(
        data = {'value':meta_values},
        index = Index(meta_keys, name='key')
        )


  def make_columns_meta(self):
    """Generates self.columns from self.data .
    """

    input_names = list( self.data.index.names ) + self.data.columns.tolist()
    db_names = [ to_alphanumeric( name.lower() ).replace(' ','_') for name in input_names ]
    output_names = input_names
    mc_names = [ to_alphanumeric( name.title() ).replace('_',' ') for name in db_names ]
    an_names = mc_names
    mc_tag = [ to_alphanumeric( name.upper() ).replace('_','') for name in db_names ]
    mc_dtypes = []

    dtypes = (
        [ self.data.index.dtype.name ] * len( self.data.index.names ) +
        [ dtype.name for dtype in self.data.dtypes ]
        )

    for dtype in dtypes:

      if dtype == 'object':
        mc_dtypes.append( 'text' )

      else:
        mc_dtypes.append( 'number' )

    output_format = []

    for dtype in dtypes:

      if dtype[:5] == 'float':
        output_format.append( 'float' )

      else:
        output_format.append( 'str' )

    mc_col_nums = list( range( len( input_names ) ) )

    self.columns = DataFrame(
        data={
            'input_name' : input_names,
            'mc_name' : mc_names,
            'an_name' : an_names,
            'dtype' : dtypes,
            'mc_display_order' : mc_col_nums,
            'mc_tag' : mc_tag,
            'mc_dtypes' : mc_dtypes,
            'output_name' : output_names,
            'output_width' : [20] * len( input_names )
            },
        index=Index( db_names, name='db_name')
        )


  def convert_meta_values_from_json(self):
    """Method: converts self.meta values from JSON string where possible.
    """
    for value in self.meta.keys():

        if not self.meta[value] is None:

          try:
            self.meta[ value ] = loads( self.meta[ value ] )

          except:
            pass


  def convert_meta_values_to_json(self):
    """Method: converts self.meta values to JSON string where possible.
    Returns: (dict) of meta
    """
    meta = self.meta.copy()

    for value in meta.keys():

      if type(meta[value]) in [list,dict]:
        meta[ value ] = dumps( meta[ value ] )

    return meta


  def generate_slice_columns_meta(self, columns_list, column_names_group):
    """Method: returns a slice of DataFrame columns

    params:
      columns_list: (list) of columns to include in DataFrame
      column_names_group: (string) name of column heading for columns_list values
    Returns: (DataFrame) columns table
    """

    if column_names_group in self.columns.columns:
      return self.columns.loc[ self.columns[ column_names_group ].isin( columns_list ) ].copy()

    elif column_names_group in self.columns.index.names:
      return self.columns.loc[ self.columns.index.isin( columns_list ) ].copy()

    else:
      raise ValueError( f'{dumps(column_names_group)} not found in either columns or index' )


def to_alphanumeric( text ):
  """Function: strips non-alphanumeric text from string

  params:
    text: (string) with text
  Returns:
    (string)
  """
#  return sub('/^[a-z\d\-_\s]+$/i',' ',text).strip()
  return sub(r'[^a-zA-Z0-9_ ]',r'',text).strip()


def backup(self):
  """Function: generates a backup of all the files in the instance directory

  Output:
    (tar.gz) backup saved in archive folder and datetime stamped
  """
  files = [ f for f in listdir('.') if path.isfile(f) ]
  folders = [ f for f in listdir('.') if path.isdir(f) and f != 'archive' ]
  tar = tarfile_open(f"archive/{self.meta['name']}_{now()}.gz")

  for folder in folders:
    tar.add( folder )

  for file in files:
    tar.add( file )

  tar.close()


def now():
  """Function: returns a datetime stamp
  """
  return str(datetime.now())[:-7].replace('-','').replace(' ','_').replace(':','')

def today():
  """Function: returns a datetime stamp for today
  """
  return datetime.today()

