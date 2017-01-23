import pandas as pd
import json
from flask import Flask, request
from flask_restful import Resource, Api
from marshmallow import Schema, fields, pprint
from marshmallow.decorators import validates_schema

REGISTERED_TYPES = ['real', 'integer', 'discrete']

class DatatypeSchema(Schema):
    name = fields.Str()
    omittable = fields.Bool
    description = fields.Str()
    type = fields.Str()

    @validates_schema
    def validate_type(self, data):
        if data['type'] not in REGISTERED_TYPES:
            raise ValidationError('{}: Unknown type.'.format(data['type']))

class DatasetSchema(Schema):
    name = fields.Str()
    datatypes = fields.Nested(DatatypeSchema, many=True)
    observations = fields.List(fields.Dict)

    @validates_schema
    def validate_observations(self, data):
        for observation in data['observations']:
            if observation['name'] not in [ t.name for t in data['datatypes'] ]:
                raise ValidationError('{}: Not among the registered data types for the dataset.'.format(observation['name']))

class Dataset(object):
    def __init__(self, name, datatypes, data=None):
        self.name = name,
        self.datatypes = [Dataset.__validate_datatype(dtype) for dtype in datatypes]
        self.observations = {}
        self._columns = [dtype['name'] for dtype in datatypes]
        self._dataframe = pd.DataFrame(columns=self._columns, data=data)

    def __repr__(self):
        return "Data Types: " + self.datatypes.__repr__() + "\n" + self._dataframe.__repr__()

    def add_observation(self, obs):
        for dtype in self.datatypes:
            value = obs.get(dtype['name'])
            if value == None and not dtype['omittable']:
                raise ValueError("Missing non-omittable value {}.".format(dtype['name']))
        self._dataframe = self._dataframe.append(obs, ignore_index=True)


class DatasetSerializer(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Dataset):
            return {'name': obj.name, 'datatypes': obj.datatypes, 'observations': obj.observations}
        return super().default(obj)


class DatasetDeserializer(json.JSONDecoder):
    def decode(self, obj):
        j = json.loads(obj)
        return Dataset(j['name'], j['datatypes'], j['observations'])


class DatasetCollectionResource(Resource):
    Datasets = {}
    def get(self):
        return json.dumps(DatasetCollectionResource.Datasets, cls=DatasetSerializer)

    def post(self):
        try: 
            json_data = request.get_json(force=True)
            obj = json.load(json_data, cls=DatasetDeserializer)
        except Exception as e:
            return { "message": str(e)}, 500

        return obj, 200


if __name__ == "__main__":
    app = Flask(__name__)
    api = Api(app)
    api.add_resource(DatasetCollectionResource, '/datasets/')
    app.run()
