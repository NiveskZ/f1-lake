# %%
import matplotlib.pyplot as plt
import mlflow
import pandas as pd

from feature_engine import imputation

from sklearn import ensemble
from sklearn import metrics
from sklearn import model_selection
from sklearn import pipeline
# %%

mlflow.set_tracking_uri("http://127.0.0.1:5000")
mlflow.set_experiment(experiment_id=1)
# %%
df = pd.read_csv("../data/abt_f1_drivers_champion.csv",sep=';')
df.head()
# %%
## SAMPLING

df['year'] = df['dtref_life'].apply(lambda x: x.split("-")[0]).astype(int)
df_oot = df[df['year'] == 2025].copy()
df_oot

df_analytics = df[df['year'] < 2025].copy()
# %%
# Removendo as ultimas corridas de cada ano, tentando tratar possível leakage
df_year_round = df_analytics[['year','dtref_life']].drop_duplicates()
df_year_round['row_number'] = df_year_round.sort_values('dtref_life', ascending=False).groupby('year').cumcount()
df_year_round[['row_number','dtref_life', 'year']]

df_year_round = df_year_round[df_year_round['row_number'] > 1]
df_year_round.drop('row_number', axis=1, inplace=True)

# %%
df_driver_year = df_analytics[['driverid_life', 'year','flchampion']].drop_duplicates()
df_driver_year
# %%
train, test = model_selection.train_test_split(df_driver_year,
                                               random_state=42,
                                               train_size=0.8,
                                               stratify=df_driver_year['flchampion']
                                               )
# %%

df_train = train.merge(df_analytics).merge(df_year_round, how='inner')
df_test = test.merge(df_analytics).merge(df_year_round, how='inner')

# %%

features = df_train.columns[4:]
features

X_train, y_train = df_train[features], df_train['flchampion']
X_test, y_test = df_test[features], df_test['flchampion']
X_oot, y_oot = df_oot[features], df_oot['flchampion']

# %%
isna = X_train.isna().sum()
isna[isna > 0]

# %%
missing = imputation.ArbitraryNumberImputer(-10000,
                                            variables=X_train.columns.tolist())

clf = ensemble.RandomForestClassifier(min_samples_leaf=50,
                                      n_estimators=500,
                                      random_state=42,
                                      n_jobs=4)

model = pipeline.Pipeline(
    steps=[
        ('Imputation', missing),
        ('RandomForest', clf)
    ]
)

# %%
with mlflow.start_run():
    model.fit(X_train, y_train)

    y_train_pred = model.predict(X_train)
    y_train_prob = model.predict_proba(X_train)[:,1]
    roc_train = metrics.roc_curve(y_train,y_train_prob)
    auc_train = metrics.roc_auc_score(y_train,y_train_prob)

    mlflow.log_metric("ROC Train", auc_train)

    model.fit(X_test, y_test)

    y_test_pred = model.predict(X_test)
    y_test_prob = model.predict_proba(X_test)[:,1]
    roc_test = metrics.roc_curve(y_test,y_test_prob)
    auc_test = metrics.roc_auc_score(y_test,y_test_prob)

    mlflow.log_metric("ROC Teste", auc_test)

    model.fit(X_oot, y_oot)

    y_oot_pred = model.predict(X_oot)
    y_oot_prob = model.predict_proba(X_oot)[:,1]
    roc_oot = metrics.roc_curve(y_oot,y_oot_prob)
    auc_oot = metrics.roc_auc_score(y_oot,y_oot_prob)

    mlflow.log_metric("ROC Out of time", auc_oot)

    plt.figure(dpi=100)
    plt.plot(roc_train[0],roc_train[1])
    plt.plot(roc_test[0],roc_test[1])
    plt.plot(roc_oot[0],roc_oot[1])
    plt.legend([f"Treino: {auc_train:.4f}",f"Teste: {auc_train:.4f}",f"Oot: {auc_oot:.4f}"])
    plt.grid(True)
    plt.title("Curva ROC")
    plt.savefig("../img/roc_curve.png")
    mlflow.log_artifact("../img/roc_curve.png")

    features_importances = pd.Series(clf.feature_importances_, index=X_train.columns)
    features_importances = features_importances.sort_values(ascending=False)
    features_importances.to_markdown("feature_importance.md")
    mlflow.log_artifact("feature_importance.md")

    model.fit(df[features], df['flchampion'])

    mlflow.sklearn.log_model(model, name='model')
# %%
