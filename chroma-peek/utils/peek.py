import chromadb 
import pandas as pd

class ChromaPeek:
    def __init__(self, path):
        self.client = chromadb.PersistentClient(path)

    ## function that returs all collection's name
    def get_collections(self):
        collections = []
        for i in self.client.list_collections():
            collections.append(i.name)
        return collections

    def get_collection_data(self, collection_name, dataframe=False):
        data = self.client.get_collection(name=collection_name).get()
        
        # Ensure all values are lists
        for key in data.keys():
            if not isinstance(data[key], list):
                data[key] = [data[key]]
        
        # Ensure all arrays in data have the same length
        min_length = min(len(v) for v in data.values() if isinstance(v, list))
        for key in data.keys():
            data[key] = data[key][:min_length]

        if dataframe:
            return pd.DataFrame(data)
        return data

    def query(self, query_str, collection_name, k=3, dataframe=False):
        collection = self.client.get_collection(collection_name)
        res = collection.query(
            query_texts=[query_str], n_results=min(k, len(collection.get()))
        )
        out = {}
        for key, value in res.items():
            if value:
                out[key] = value[0]
            else:
                out[key] = value
        if dataframe:
            return pd.DataFrame(out)
        return out
