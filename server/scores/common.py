

def map_column_alignment(df, alignment_series, column):
    return df.join(alignment_series.rename(column), on=column, how='left', lsuffix='_orig')


def reverse_alignment_series(alignment_series):
    return alignment_series.reset_index().set_index(alignment_series.name).iloc[:, 0]


def add_alignment_image(net_df, alignment_series):
    src, tgt = net_df.columns[:2]

    return net_df \
        .pipe(map_column_alignment, alignment_series, src) \
        .pipe(map_column_alignment, alignment_series, tgt)

