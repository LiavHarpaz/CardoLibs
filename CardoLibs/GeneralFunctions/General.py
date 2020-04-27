import functools
from typing import Any, Callable, Generator, List, Union, Tuple

import pandas as pd
import numpy as np
import pyspark
from CardoExecutor.Common.CardoDataFrame import CardoDataFrame, CardoPandasDataFrame


def union_dataframes(*dataframes: Union[CardoDataFrame, List[CardoDataFrame], Tuple[CardoDataFrame]]):
	"""
	:param dataframes:
	:return: union of all those dataframes
	"""
	if isinstance(dataframes[0], list) or isinstance(dataframes[0], tuple):
		return union_dataframes(*[dataframe for many_dataframe in dataframes for dataframe in many_dataframe])

	else:
		if isinstance(dataframes[0]._inner, pyspark.sql.DataFrame) or isinstance(dataframes[0]._inner, pyspark.RDD):
			unioned = functools.reduce(
				lambda df1, df2: CardoDataFrame(df1.union(df2.select(df1.columns))),
				dataframes)
			return CardoDataFrame(unioned)
		if isinstance(dataframes[0]._inner, pd.DataFrame):
			unioned = pd.concat(dataframes, axis=0)
			return CardoPandasDataFrame(unioned)


def generic_fillna(df: pd.DataFrame, fill_zeros: bool = True, value: Any = np.nan, *args, **kwargs) -> pd.DataFrame:
	default_fill = {None: np.nan, 'nan': np.nan, 'None': np.nan, 'none': np.nan,
					'Nan': np.nan, 'NAN': np.nan, '': np.nan}
	if fill_zeros:
		default_fill.update({'0': np.nan})
	return df.replace(default_fill).fillna(*args, value=value, **kwargs)


def show(df: pd.DataFrame, max_rows: int = 1000, max_columns: int = 1000, col_width: int = -1):
	try:
		from IPython import get_ipython
		from IPython.display import display
		get_ipython()
		with pd.option_context("display.max_rows", max_rows, 'display.max_columns', max_columns, 'display.max_colwidth',
							   col_width):
			display(df)
	except:
		print(df)
