# Recommender-System - MovieLens Dataset

This application is designed to implement Movie Recommendation using Java Machine Learning libraries.

I have used Matrix Factorization Technique to predict the possible new movie recommendations for new user based on their past ratings.

Apache Spark has various machine learning libraries to build prediction models. Out of them there is ALS based matrix factorization using Collaborative Filtering.

Below is the approach followed:

1. Take the 100K MovieLens Dataset to train the model 
2. Use different rank values and get the best model with RMSE value.
3. Use this rank and default iteration of 20 to build the model using larger data set of 22 million users.
4. Create new user and assign few ratings with this users id.
5. train the model with new user ratings.
6. predict new movies for this user based on past ratings.
