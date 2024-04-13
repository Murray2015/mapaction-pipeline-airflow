from .utils import make_dir_download_csv


def ourairports(data_in_directory, data_out_directory):
    url = "https://davidmegginson.github.io/ourairports-data/airports.csv"
    make_dir_download_csv(url, data_in_directory + "/ourairports", "ourairports.csv")
    # TODO: now need to transform (extract) the data by country polygon
