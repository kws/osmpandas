import logging

import pandas as pd
from pandas.api.extensions import register_dataframe_accessor

logger = logging.getLogger(__name__)


class OSMDataFrame(pd.DataFrame):
    _metadata = ["tag_dataframe"]

    @property
    def _constructor(self):
        return OSMDataFrame


@register_dataframe_accessor("osm")
class OSMAccessor:
    def __init__(self, pandas_obj):
        self._obj = pandas_obj

    def expand_tags(self, *args) -> pd.DataFrame:
        first_arg = args[0] if args else None
        if isinstance(first_arg, pd.DataFrame):
            tags_df = first_arg
            args = args[1:]
        elif hasattr(self._obj, "tag_dataframe"):
            tags_df = self._obj.tag_dataframe
        else:
            raise ValueError(
                "No tag dataframe found. Either use an OSMDataFrame or provide a tag "
                "dataframe as the first argument"
            )

        if args:
            tags_df = tags_df[tags_df.key.isin(args)]

        tags_df = tags_df.pivot(index="ref", columns="key", values="value")
        return self._obj.merge(tags_df, left_on="id", right_index=True, how="left")
