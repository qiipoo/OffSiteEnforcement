
class ColumnDefinition:
    col_index = ''
    col_name = ''
    col_type = ''
    col_len = ''
    col_not_null = ''
    col_default_val = ''
    col_key = ''
    col_comment = ''

    def __init__(self, name, typ, null, comment):
        self.col_name = name
        self.col_type = typ
        self.col_not_null = null
        self.col_comment = comment


class Table(ColumnDefinition):
    table_name = ''
    table_columns = []
    table_comment = ''

    def __init__(self, name, columns, comment):
        self.table_name = name
        self.table_columns = columns
        self.table_comment = comment
