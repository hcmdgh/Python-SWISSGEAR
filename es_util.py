import requests
from pprint import pprint as _pprint 
import typing 

# 当请求成功时HTTP状态码的范围
SUCCESS_CODE = range(200, 300)


class UnknownError(RuntimeError):
    def __init__(self, requests_resp):
        super().__init__()

        print("【出现未知错误】")
        print(f"状态码：{requests_resp.status_code}")
        print("响应体：")
        _pprint(requests_resp.json())


class IdAlreadyExistError(RuntimeError):
    pass 


class IdNotExistError(RuntimeError): 
    pass


class IndexAlreadyExistError(RuntimeError): 
    pass


class IndexNotExistError(RuntimeError): 
    pass


class EsClient:
    def __init__(self, host: str):
        """
        初始化ES客户端，须指定主机地址，支持如下格式：
        1. URL：http://192.168.0.83:9200
        1. IP地址+端口号：192.168.0.83:9200
        """
        host = host.strip().rstrip('/')

        assert not host.startswith('https')
        
        if not host.startswith('http://'):
            host = 'http://' + host 
            
        self.host = host 

    def get_index(self, index_name: str, type_name: str = '_doc') -> 'EsIndex':
        return EsIndex(client=self, index_name=index_name, type_name=type_name)

    def get_client_info(self, print: bool = True) -> dict:
        resp = requests.get(self.host)
        resp_json = resp.json()

        if print:
            _pprint(resp_json)

        return resp_json
    
    
def _extract_entry_id(entry: dict) -> typing.Union[None, str, int]:
    """
    从表示文档的dict中提取_id值，并删除之。
    """
    _id = None
    
    if 'id' in entry:
        _id = entry['id']
        del entry['id']

    if '_id' in entry:
        _id = entry['_id']
        del entry['_id']

    return _id 
    
    
class EsIndex:
    def __init__(self, client: EsClient, index_name: str, type_name: str = '_doc'):
        self.host = client.host
        self.index_name = index_name
        self.type_name = type_name

    def delete_index(self) -> bool:
        """
        删除当前索引。
        
        如果索引不存在，返回False。
        """
        resp = requests.delete(f'{self.host}/{self.index_name}')

        if resp.status_code in SUCCESS_CODE:
            return True
        elif resp.status_code == 404:
            return False 
        else:
            raise UnknownError(resp)

    def get_mapping(self) -> dict:
        resp = requests.get(f'{self.host}/{self.index_name}/_mapping')

        if resp.status_code not in SUCCESS_CODE:
            raise UnknownError(resp)

        return resp.json()
    
    def insert_one(self, entry: dict, refresh: bool = False) -> str:
        """
        新增一个文档。该文档无需提供_id，将自动生成新的_id并返回。

        可以指定是否刷新索引，刷新索引将使得当前修改立即可被搜索。
        """
        assert not _extract_entry_id(entry)
        
        resp = requests.post(url=f'{self.host}/{self.index_name}/{self.type_name}',
                             json=entry)
        if resp.status_code not in SUCCESS_CODE:
            raise UnknownError(resp)
        
        if refresh:
            self.refresh()
            
        return resp.json()['_id']

    def save_one(self, entry: dict, refresh: bool = False) -> str:
        """
        新增一个文档。该文档必须提供_id，将覆盖已有_id的文档。
        
        可以指定是否刷新索引，刷新索引将使得当前修改立即可被搜索。
        """
        
        _id = _extract_entry_id(entry)
        assert _id 
        
        resp = requests.put(url=f'{self.host}/{self.index_name}/{self.type_name}/{_id}',
                            json=entry)
        if resp.status_code not in SUCCESS_CODE:
            raise UnknownError(resp)
        
        if refresh:
            self.refresh()

        return resp.json()['_id']

    def delete_by_id(self, _id: typing.Union[str, int]) -> bool:
        """
        根据_id删除文档。删除成功返回真，删除失败（即_id不存在）返回假。
        """
        
        assert _id 

        resp = requests.delete(f'{self.host}/{self.index_name}/{self.type_name}/{_id}')

        if resp.status_code == 404:
            return False 
        elif resp.status_code in SUCCESS_CODE:
            return True 
        else:
            raise UnknownError(resp)

    def get_by_id(self, _id: typing.Union[str, int]) -> dict:
        """
        根据_id查询文档。
        
        如果_id不存在，抛出IdNotExistError异常。
        """
        
        assert _id
         
        resp = requests.get(f'{self.host}/{self.index_name}/{self.type_name}/{_id}')

        if resp.status_code in SUCCESS_CODE:
            resp_json = resp.json()
            entry = resp_json['_source']
            entry['_id'] = resp_json['_id']
            return entry 
        elif resp.status_code == 404:
            raise IdNotExistError
        else:
            raise UnknownError(resp)
        
    def delete_all(self):
        """
        删除该索引中所有文档。
        """
        
        resp = requests.post(url=f'{self.host}/{self.index_name}/{self.type_name}/_delete_by_query',
                             json={
                                 'query': {
                                     'match_all': {}
                                 }
                             })

        if resp.status_code not in SUCCESS_CODE:
            raise UnknownError(resp)
        
    def refresh(self):
        """
        刷新该索引，使得对索引的修改立即可被搜索。
        """
        resp = requests.post(f'{self.host}/{self.index_name}/_refresh')

        if resp.status_code == 404:
            raise IndexNotExistError
        
        if resp.status_code not in SUCCESS_CODE:
            raise UnknownError(resp)

    def count(self, query: typing.Optional[dict] = None) -> int:
        """
        查询符合条件的文档数量。
        
        如果不指定查询条件，返回当前索引的所有文档数量。
        """
        if query:
            request_body = { 'query': query }
        else:
            request_body = None 

        resp = requests.get(url=f'{self.host}/{self.index_name}/{self.type_name}/_count',
                            json=request_body)
        
        if resp.status_code not in SUCCESS_CODE:
            raise UnknownError(resp)

        resp_json = resp.json()
        
        return resp_json['count']

    def scroll_search(self, query: dict, page_size: int = 1000) -> typing.Iterable[dict]:
        """
        滚动搜索，用于解决ES不能深度分页的问题。
        """
        scroll_id = None
        
        while True:
            if not scroll_id:
                request_body = {
                    'size': page_size,
                    'query': query,
                    'sort': ['_doc'],
                }
                request_url = f'{self.host}/{self.index_name}/{self.type_name}/_search?scroll=17m'
            else:
                request_body = {
                    'scroll': '17m',
                    'scroll_id': scroll_id,
                }
                request_url = f'{self.host}/_search/scroll'

            resp = requests.get(url=request_url, json=request_body)

            if resp.status_code not in SUCCESS_CODE:
                raise UnknownError(resp)
            
            resp_json = resp.json()
            scroll_id = resp_json.get('_scroll_id')

            entries = resp_json['hits']['hits']
            
            if not entries:
                break
            
            for entry in entries:
                formatted_entry = entry['_source']
                formatted_entry['_id'] = entry['_id']
                yield formatted_entry

    def get_all(self, page_size: int = 1000) -> typing.Iterable[dict]:
        """
        查询索引中所有文档，以生成器的形式依次返回每个文档。
        """
        
        iter_ = self.scroll_search(
            query={ 'match_all': {} },
            page_size=page_size,
        )
        for entry in iter_:
            yield entry

    def create_mapping(self, properties: dict, delete_index: bool = False):
        """
        创建mapping。可以指定创建前是否删除索引。支持以下特殊mapping：
        
        1. cn_text
        2. cn_text_keyword
        3. text_keyword
        
        其中中文分词引擎为ik分词器：https://github.com/medcl/elasticsearch-analysis-ik
        """
        
        if delete_index:
            self.delete_index()
        
        _properties = dict(properties)
        for key, value in _properties.items():
            type_ = value['type']
            if type_ == 'cn_text':
                _properties[key] = {
                    "type": "text",
                    "analyzer": "ik_max_word",
                    "search_analyzer": "ik_smart",
                }
            elif type_ == 'cn_text_keyword':
                _properties[key] = {
                    "type": "text",
                    "analyzer": "ik_max_word",
                    "search_analyzer": "ik_smart",
                    "fields": {
                        "keyword": { "type": "keyword", "ignore_above": 256 },
                    },
                }
            elif type_ == 'text_keyword':
                _properties[key] = {
                    "type": "text",
                    "fields": {
                        "keyword": { "type": "keyword", "ignore_above": 256 },
                    },
                }

        if self.type_name != '_doc':
            request_body = {
                'mappings': {
                    self.type_name: {
                        'dynamic': 'strict',
                        'properties': _properties,
                    }
                }
            }
        else:
            request_body = {
                'mappings': {
                    'dynamic': 'strict',
                    'properties': _properties,
                }
            }
            
        resp = requests.put(f'{self.host}/{self.index_name}', json=request_body)

        if resp.status_code not in SUCCESS_CODE:
            raise UnknownError(resp)

    def query_by_field(self, 
                       field: str, 
                       val, 
                       method: str = 'match', 
                       size: int = 100,
                       return_score: bool = False) -> list[dict]:
        resp = requests.get(url=f'{self.host}/{self.index_name}/{self.type_name}/_search',
                            json={
                                'query': {
                                    method: {
                                        field: val 
                                    }
                                },
                                'size': size,
                            })
        
        if not resp.status_code in SUCCESS_CODE:
            raise UnknownError(resp)
        
        resp_json = resp.json()
        entries = []
        for entry in resp_json['hits']['hits']:
            formatted_entry = entry['_source']
            formatted_entry['_id'] = entry['_id']
            if return_score:
                formatted_entry['_score'] = entry['_score']
            entries.append(formatted_entry)
            
        return entries
    
    def query(self, query_body: dict, size: int = 100, return_score: bool = False) -> list[dict]:
        resp = requests.get(url=f'{self.host}/{self.index_name}/{self.type_name}/_search',
                            json={
                                'query': query_body,
                                'size': size,
                            })
        
        if not resp.status_code in SUCCESS_CODE:
            raise UnknownError(resp)
        
        resp_json = resp.json()
        entries = []
        for entry in resp_json['hits']['hits']:
            formatted_entry = entry['_source']
            formatted_entry['_id'] = entry['_id']
            if return_score:
                formatted_entry['_score'] = entry['_score']
            entries.append(formatted_entry)
            
        return entries
