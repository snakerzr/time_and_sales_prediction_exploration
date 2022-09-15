# time_and_sales_prediction_exploration
Study project for price movement predictions with ML.

The equity is RTS futures. [link](https://www.moex.com/ru/derivatives/equity/indices/#:~:text=24-,%D0%A4%D1%8C%D1%8E%D1%87%D0%B5%D1%80%D1%81%D0%BD%D1%8B%D0%B9%20%D0%BA%D0%BE%D0%BD%D1%82%D1%80%D0%B0%D0%BA%D1%82%20%D0%BD%D0%B0%20%D0%98%D0%BD%D0%B4%D0%B5%D0%BA%D1%81%20%D0%A0%D0%A2%D0%A1,-RTS%2D9.22)

The main idea is to combine predictions from 2 models.  
First will predict if next high will be higher than current close + TP amount. (Take profit level hit)  
Second - if the next low will be lower than current close - SP amount. (Stop loss level hit)

Long position is opened whenever the probability of first model is high enough and - of second is low enough.

Progress:  
[x] - Data acquisition from market directly including level 1 and level 2 data  
[x] - Simple preprocessing. Tick to 1min, volume, dollar bars.  
[x] - ML prototyping sketches  
[x] - Backtesting framework sketch  
[ ] - Advanced data preprocessing

Additional stuff done:  
Studied different approaches to gather market book data with multiprocessing. (deemed unnecessary as there is simpler way to do it.)
