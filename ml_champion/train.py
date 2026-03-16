# %%
import pandas as pd
from sklearn import model_selection
from feature_engine import imputation
from sklearn import ensemble
from sklearn import metrics

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

df_train = train.merge(df_analytics)
df_test = test.merge(df_analytics)

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
X_train_transform = missing.fit_transform(X_train)

# %%

clf = ensemble.RandomForestClassifier(min_samples_leaf=50,
                                      n_estimators=500,
                                      random_state=42,
                                      n_jobs=4)

clf.fit(X_train_transform, y_train)

# %%

y_train_pred = clf.predict(X_train_transform)
y_train_prob = clf.predict_proba(X_train_transform)[:,1]

auc_train = metrics.roc_auc_score(y_train,y_train_prob)
roc_train = metrics.roc_curve(y_train,y_train_prob)
print("AUC train:", auc_train)

# %%

X_test_transform = missing.transform(X_test)
y_test_pred = clf.predict(X_test_transform)
y_test_prob = clf.predict_proba(X_test_transform)[:,1]

auc_test = metrics.roc_auc_score(y_test,y_test_prob)
roc_test = metrics.roc_curve(y_test,y_test_prob)
print("AUC teste:", auc_test)

# %%
X_oot_transform = missing.transform(X_oot)
y_oot_pred = clf.predict(X_oot_transform)
y_oot_prob = clf.predict_proba(X_oot_transform)[:,1]

auc_oot = metrics.roc_auc_score(y_oot,y_oot_prob)
roc_oot = metrics.roc_curve(y_oot,y_oot_prob)
print("AUC Out of Time:", auc_oot)
# %%

import matplotlib.pyplot as plt

plt.plot(roc_train[0],roc_train[1])
plt.plot(roc_test[0],roc_test[1])
plt.plot(roc_oot[0],roc_oot[1])
plt.legend([f"Treino: {auc_train:.4f}",f"Teste: {auc_train:.4f}",f"Oot: {auc_oot:.4f}"])
plt.grid(True)

# %%

features_importances = pd.Series(clf.feature_importances_, index=X_train_transform.columns)
features_importances.sort_values(ascending=False)