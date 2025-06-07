import pyspark.sql.functions as F


class Operator:
    def __init__(self, operator_info: dict, column_mapping: dict):
        self.column_mapping = column_mapping
        self.field_name = column_mapping[operator_info['field_name']]
        self.operator = operator_info['operator']
        self.value = operator_info['value']

        self.time_period_type = None
        if 'time_period' in self.operator:
            # This can be generalized further, depending on the other operators that will be supported
            try:
                self.time_period_type = operator_info['time_period_type']
            except KeyError:
                raise KeyError(
                    f"Time period operator ({self.operator}) must be used together with a time_period_type argument!")

    def transform(self, df):
        if self.operator == 'eq':
            return self._equals(df)
        elif self.operator == 'last_time_period':
            return self._date_later_than(df)
        else:
            raise ValueError(
                f"Currently {self.operator} is not supported! Supported operators are 'eq' and 'last_time_period'")

    def _date_later_than(self, df):
        cutoff_expression = f"current_timestamp() - INTERVAL {self.value} {self.time_period_type}"
        start_time_col = F.expr(cutoff_expression)
        return df.filter(F.col(self.field_name) >= start_time_col)

    def _equals(self, df):
        return df.filter(df[self.field_name] == self.value)
