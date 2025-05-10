from tensorflow.keras.models import load_model
import joblib
import numpy as np
class StockModelInference(object):
    def __init__(self, model_path="./models/model.h5", scalar_path="./models/scaler.save"):
        self.scaler = joblib.load(scalar_path)
        # Load lại model
        self.model = load_model(model_path)
        self.last_column = None
    def preprocess(self, data):
        """_summary_

        Args:
            data (np.array): Shape [sequence_length, num_col] 
                sequence_length = 6: num input months
                num_col = 1: Column "Close" in csv file
        Return:
            results: Shape [sequence_length, num_col] 
                sequence_length = 6: num input months
                num_col = 1: Column "Close" in csv file
        """
        self.last_column = data[-1,0]

        scaler_data = self.scaler.fit_transform(data)
        return np.reshape(scaler_data, (1, scaler_data.shape[0], scaler_data.shape[1]))
    def predict(self, data):
        predictions = self.model.predict(data)
        return predictions
    def get_stock_trend(self, predicts):
        inverse_predicts = predicts.flatten()
     
        if self.last_column is None:
            return []
        trend = []
        if self.last_column >= inverse_predicts[0]:
            trend.append(0)
        else:
            trend.append(1)
        for i in range(0,len(inverse_predicts[:-1])):
            tr = 1 if inverse_predicts[i] < inverse_predicts[i+1] else 0
            trend.append(tr)
        return trend
    def postprocess(self,predictions):
        """_summary_

        Args:
            data (np.array): Shape [sequence_length, num_col] 
                sequence_length = 6: num input months
                num_col = 1: Column "Close" in csv file
        Return:
            results: Shape [1, num_col] 
                num_col = 1: Column "Close" next month prediction 
        """
        predicts = self.scaler.inverse_transform(predictions)
        return predicts
    def pipeline(self, data, get_trend=False):
        pre_data = self.preprocess(data)
        print(pre_data.shape)
        predictions = self.predict(pre_data)
        print(predictions.shape)
        predictions = self.postprocess(predictions)
        trend = None
        if get_trend:
            trend = self.get_stock_trend(predictions)
        return predictions, trend

if __name__ == "__main__":
    inference = StockModelInference()
    data = np.array([[190.23676],
                        [201.08466],
                        [188.34004],
                        [172.4715 ],
                        [187.72069],
                        [156.33638],])
    print(inference.pipeline(data))