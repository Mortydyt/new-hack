from .base_parser import BaseParser
from .csv_parser import CSVParser
from .json_parser import JSONParser
from .xml_parser import XMLParser
from ..models.schemas import DataFormat


def get_parser(format: DataFormat) -> BaseParser:
    parsers = {
        DataFormat.CSV: CSVParser,
        DataFormat.JSON: JSONParser,
        DataFormat.XML: XMLParser
    }
    return parsers[format]()