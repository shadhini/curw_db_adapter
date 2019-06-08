
def create_csv_like_txt(file_name, data):
    """
    Create txt file in the format of csv file
    :param file_name: file_path/file_name
    :param data: list of lists
    :return:
    """
    with open(file_name, 'w') as f:
        for _list in data:
            for i in range(len(_list)-1):
                #f.seek(0)
                f.write(str(_list[i]) + ',')
            f.write(str(_list[len(_list)-1]))
            f.write('\n')

        f.close()
