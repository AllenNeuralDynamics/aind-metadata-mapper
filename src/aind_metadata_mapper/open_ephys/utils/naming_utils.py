""" Utils to process naming of stimulus columns"""

import numpy as np
import warnings
import logging
import aind_metadata_mapper.open_ephys.utils.constants as constants

logger = logging.getLogger(__name__)


def drop_empty_columns(table):
    """Remove from the stimulus table columns whose values are all nan"""

    to_drop = []

    for colname in table.columns:
        if table[colname].isna().all():
            to_drop.append(colname)

    table.drop(columns=to_drop, inplace=True)
    return table


def collapse_columns(table):
    """merge, where possible, columns that describe the same parameter. This
    is pretty conservative - it only matches columns by capitalization and
    it only overrides nans.
    """

    colnames = set(table.columns)

    matches = []
    for col in table.columns:
        for transformed in (col.upper(), col.capitalize()):
            if transformed in colnames and col != transformed:
                col_notna = ~(table[col].isna())
                trans_notna = ~(table[transformed].isna())
                if (col_notna & trans_notna).sum() != 0:
                    continue

                mask = ~(col_notna) & (trans_notna)

                matches.append(transformed)
                table.loc[mask, col] = table[transformed][mask]
                break

    table.drop(columns=matches, inplace=True)
    return table


def add_number_to_shuffled_movie(
    table,
    natural_movie_re=constants.GENERIC_MOVIE_RE,
    template_re=constants.SHUFFLED_MOVIE_RE,
    stim_colname="stim_name",
    template="natural_movie_{}_shuffled",
    tmp_colname="__movie_number__",
):
    """
    Adds a number to a shuffled movie stimulus name, if possible.

    Parameters
    ----------
    table : pd.DataFrame
        the incoming stimulus table
    natural_movie_re : re.Pattern, optional
        regex that matches movie stimulus names
    template_re : re.Pattern, optional
        regex that matches shuffled movie stimulus names
    stim_colname : str, optional
        the name of the dataframe column that contains stimulus names
    template : str, optional
        the template's name
    tmp_colname : str, optional
        the name of the template column to use

    Returns
    -------
    table : pd.DataFrame
        the stimulus table with the shuffled movie names updated

    """

    if not table[stim_colname].str.contains(constants.SHUFFLED_MOVIE_RE).any():
        return table
    table = table.copy()

    table[tmp_colname] = table[stim_colname].str.extract(
        natural_movie_re, expand=True
    )["number"]

    unique_numbers = [
        item for item in table[tmp_colname].dropna(inplace=False).unique()
    ]
    if len(unique_numbers) != 1:
        raise ValueError(
            "unable to uniquely determine a movie number for this session. "
            + f"Candidates: {unique_numbers}"
        )
    movie_number = unique_numbers[0]

    def renamer(row):
        """
        renames the shuffled movie stimulus according to the template

        Parameters
        ----------
        row : pd.Series
            a row in the stimulus table

        Returns
        -------
        table : pd.DataFrame
            the stimulus table with the shuffled movie names updated
        """
        if not isinstance(row[stim_colname], str):
            return row[stim_colname]
        if not template_re.match(row[stim_colname]):
            return row[stim_colname]
        else:
            return template.format(movie_number)

    table[stim_colname] = table.apply(renamer, axis=1)
    logger.debug(table.keys())
    table.drop(columns=tmp_colname, inplace=True)
    return table


def standardize_movie_numbers(
    table,
    movie_re=constants.GENERIC_MOVIE_RE,
    numeral_re=constants.NUMERAL_RE,
    digit_names=constants.DIGIT_NAMES,
    stim_colname="stim_name",
):
    """Natural movie stimuli in visual coding are numbered using words, like
    "natural_movie_two" rather than "natural_movie_2". This function ensures
    that all of the natural movie stimuli in an experiment are named by that
    convention.

    Parameters
    ----------
    table : pd.DataFrame
        the incoming stimulus table
    movie_re : re.Pattern, optional
        regex that matches movie stimulus names
    numeral_re : re.Pattern, optional
        regex that extracts movie numbers from stimulus names
    digit_names : dict, optional
        map from numerals to english words
    stim_colname : str, optional
        the name of the dataframe column that contains stimulus names

    Returns
    -------
    table : pd.DataFrame
        the stimulus table with movie numerals having been mapped to english
        words

    """

    def replace(match_obj):
        """
        replaces the numeral in a movie stimulus name with its english
        equivalent

        Parameters
        ----------
        match_obj : re.Match
            the match object

        Returns
        -------
        str
            the stimulus name with the numeral replaced by its english
            equivalent

        """
        return digit_names[match_obj["number"]]

    # for some reason pandas really wants us to use the captures
    warnings.filterwarnings("ignore", "This pattern has match groups")
    warnings.filterwarnings("ignore", category=UserWarning)

    movie_rows = table[stim_colname].str.contains(movie_re, na=False)
    table.loc[movie_rows, stim_colname] = table.loc[
        movie_rows, stim_colname
    ].str.replace(numeral_re, replace, regex=True)

    return table


def map_stimulus_names(table, name_map=None, stim_colname="stim_name"):
    """Applies a mappting to the stimulus names in a stimulus table

    Parameters
    ----------
    table : pd.DataFrame
        the input stimulus table
    name_map : dict, optional
        rename the stimuli according to this mapping
    stim_colname: str, optional
        look in this column for stimulus names

    """

    if name_map is None:
        return table

    name_map[np.nan] = "spontaneous"

    table[stim_colname] = table[stim_colname].replace(
        to_replace=name_map, inplace=False
    )

    name_map.pop(np.nan)

    return table


def map_column_names(table, name_map=None, ignore_case=True):
    """
    Maps column names in a table according to a mapping.

    Parameters
    ----------
    table : pd.DataFrame
        the input table
    name_map : dict, optional
        mapping from old names to new names
    ignore_case : bool, optional
        ignore case when mapping column names

    Returns
    -------
    table : pd.DataFrame
        the table with column names mapped

    """
    if ignore_case and name_map is not None:
        name_map = {key.lower(): value for key, value in name_map.items()}

        def mapper(name):
            """
            Maps a column name to a new name from the map

            Parameters
            ----------
            name : str
                the column name to map

            Returns
            -------
            str
                the mapped column name
            """
            name_lower = name.lower()
            if name_lower in name_map:
                return name_map[name_lower]
            return name

    else:
        mapper = name_map

    return table.rename(columns=mapper)
