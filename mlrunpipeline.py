import mlrun

# Create or load an MLRun project
project = mlrun.get_or_create_project(name="mlrun-demo", context="./", user_project=True)

# Configure default artifact storage (if needed)
project.set_artifact_path(f"/User/artifacts")

#Define and register the training function
def train_model(context, data: mlrun.DataItem, label_column: str = "label"):
    import pandas as pd
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.metrics import accuracy_score
    import joblib

    df = data.as_df()
    X = df.drop(columns=[label_column])
    y = df[label_column]

    model = RandomForestClassifier(n_estimators=10)
    model.fit(X, y)

    predictions = model.predict(X)
    accuracy = accuracy_score(y, predictions)

    context.log_result("accuracy", accuracy)
    model_file = "model.joblib"
    joblib.dump(model, model_file)
    context.log_model("rf_model", body_path=model_file, model_file=model_file,
                      framework="sklearn", labels={"type": "classifier"})


#-----------------------
