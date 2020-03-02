from flask import Flask, request
from flask_restful import Resource, Api

app = Flask(__name__)
api = Api(app)


class HelloWorld(Resource):
    def get(self):
        try:
            return {'hello': 'world of heaven'}
        except:
            print('Error')
    
    def put(self, todo_id):
        print(request.form['data'])
        return {todo_id: "111"}


api.add_resource(HelloWorld, '/<string:todo_id>')

if __name__ == '__main__':
    app.run(debug=True)
