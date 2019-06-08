import csv


def create_csv(file_name, data):
    """
    Create new csv file using given data
    :param file_name: <file_path/file_name>.csv
    :param data: list of lists
    e.g. [['Person', 'Age'], ['Peter', '22'], ['Jasmine', '21'], ['Sam', '24']]
    :return:
    """
    with open(file_name, 'w') as csvFile:
        writer = csv.writer(csvFile)
        writer.writerows(data)

    csvFile.close()


def append_csv(file_name, row):
    """
    Append existing csv file using given data
    :param file_name: <file_path/file_name>.csv
    :param row: list of row data
    e.g. ['Jasmine', '21']
    :return:
    """
    with open(file_name, 'a') as csvFile:
        writer = csv.writer(csvFile)
        writer.writerow(row)

    csvFile.close()


def read_csv(file_name):
    """
    Read csv file
    :param file_name: <file_path/file_name>.csv
    :return: list of lists which contains each row of the csv file
    """

    with open(file_name, 'r') as f:
        data = [list(line) for line in csv.reader(f)][1:]

    return data


def delete_row(file_name, match_index, match_string):
    """
    Delete a row from csv file
    :param file_name: <file_path/file_name>.
    :param match_string: value of the field that need to be checked
    :param match_index: index of the field
    :return: update an existing csv file
    """

    with open(file_name, 'r') as f:
        data = [list(line) for line in csv.reader(f)]
    f.close()

    update_data = []

    for i in range(len(data)):
        if data[i][match_index] != match_string:
            update_data.append(data[i])

    with open(file_name, 'w') as csvFile:
        writer = csv.writer(csvFile)
        writer.writerows(update_data)

    csvFile.close()

