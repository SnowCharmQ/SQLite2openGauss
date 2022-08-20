import logging


class Properties:
    def __init__(self, file_name: str):
        self.file_name = file_name
        self.properties = {}
        try:
            pro_file = open(self.file_name, 'r', encoding='utf-8')
            for line in pro_file:
                if line.find('=') > 0:
                    strs = line.replace('\n', '').split('=')
                    self.properties[strs[0]] = strs[1]
        except Exception as e:
            logging.error(e)
            raise e
        else:
            pro_file.close()

    def get_properties(self):
        return self.properties

    @staticmethod
    def write_properties(file_name: str, properties: dict):
        try:
            properties_file = open(file_name, 'w', encoding='utf-8')
            for k in properties:
                s = k + "=" + properties[k] + "\n"
                if k == 'database.password':
                    flag = input("Save the password? [y/n]")
                    if flag.upper() == 'Y' or flag.upper() == 'YES':
                        properties_file.write(s)
                else:
                    properties_file.write(s)
        except Exception as e:
            logging.error(e)
            raise e
        else:
            properties_file.close()
