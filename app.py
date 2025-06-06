from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import desc, create_engine
from sqlalchemy.sql import func
from typing import List
import pandas as pd
from catboost import CatBoostClassifier
from loguru import logger
import os
from datetime import datetime

from database import SessionLocal
# Для формирования запроса через ORM
from table_user import User
from table_post import Post
from table_feed import Feed
# Для валидации
from schema import UserGet, PostGet, FeedGet

app = FastAPI()

def batch_load_sql(query: str) -> pd.DataFrame:
    CHUNKSIZE = 200000
    engine = create_engine(
        "postgresql://robot-startml-ro:pheiph0hahj1Vaif@"
        "postgres.lab.karpov.courses:6432/startml"
    )
    conn = engine.connect().execution_options(stream_results=True)
    chunks = []
    for chunk_dataframe in pd.read_sql(query, conn, chunksize=CHUNKSIZE):
        chunks.append(chunk_dataframe)
    conn.close()
    return pd.concat(chunks, ignore_index=True)

def get_model_path(path: str) -> str:
    if os.environ.get("IS_LMS") == "1":
        MODEL_PATH = '/workdir/user_input/model'
    else:
        MODEL_PATH = path
    return MODEL_PATH

def load_features():
    logger.info("loading liked posts")
    liked_posts_query = """
        SELECT distinct post_id, user_id
        FROM public.feed_data
        WHERE action='like'
        LIMIT 1000"""
    liked_posts = batch_load_sql(liked_posts_query)

    logger.info("loading posts features")
    posts_features = pd.read_sql(
        """SELECT * FROM public.posts_info_features
        LIMIT 1000
        """,

        con="postgresql://robot-startml-ro:pheiph0hahj1Vaif@"
        "postgres.lab.karpov.courses:6432/startml"
    )

    logger.info("loading user features")
    user_features = pd.read_sql(
        """SELECT * FROM public.user_data
        LIMIT 1000
        """,

        con="postgresql://robot-startml-ro:pheiph0hahj1Vaif@"
        "postgres.lab.karpov.courses:6432/startml"
    )

    return [liked_posts, posts_features, user_features]

def load_models():
    model_path = get_model_path(os.path.join("model", "catboost_model"))
    loaded_model = CatBoostClassifier()
    loaded_model.load_model(model_path)
    return loaded_model

logger.info("loading model")
model = load_models()
logger.info("loading features")
features = load_features()
logger.info("service is up and running")

# Эти две функции будут далее вызываться внутри endpoint, получая на вход ORM-сессию БД
def get_user_by_id(db: Session, id: int) -> User:
    return db.query(User).filter(User.id == id).one_or_none()


def get_post_by_id(db: Session, id: int) -> Post:
    return db.query(Post).filter(Post.id == id).one_or_none()


def get_feed(db: Session, id: int, limit: int, by_user_id: bool) -> Feed:
    tmp = db.query(Feed)
    if by_user_id:
        tmp = (
            tmp.filter(Feed.user_id == id)
        )
    else:
        tmp = (
            tmp.filter(Feed.post_id == id)
        )
    tmp = (
        tmp.order_by(desc(Feed.time)).
        limit(limit).
        all()
    )
    return tmp

def get_recommended_feed(id: int, time: datetime, limit: int):
    logger.info(f"user_id: {id}")
    logger.info("reading features")
    user_features = features[2].loc[features[2].user_id == id]
    user_features = user_features.drop('user_id', axis=1)

    logger.info("dropping columns")
    posts_features = features[1].drop(['index', 'text'], axis=1)
    content = features[1][['post_id', 'text', 'topic']]

    logger.info("zipping everything")
    add_user_features = dict(zip(user_features.columns, user_features.values[0]))
    logger.info("assigning everything")
    user_posts_features = posts_features.assign(**add_user_features)
    user_posts_features = user_posts_features.set_index('post_id')

    logger.info("add time info")
    user_posts_features['hour'] = time.hour
    user_posts_features['month'] = time.month

    logger.info("predicting")
    predicts = model.predict_proba(user_posts_features)[:, 1]
    user_posts_features['predicts'] = predicts

    logger.info("deleting liked posts")
    liked_posts = features[0]
    liked_posts = liked_posts[liked_posts.user_id == id].post_id.values
    filtered_ = user_posts_features[~user_posts_features.index.isin(liked_posts)]

    recommended_posts_ = filtered_.sort_values('predicts')[-limit:].index

    return [
        PostGet(**{
            "id": i,
            "text": content[content.post_id == i].text.values[0],
            "topic": content[content.post_id == i].topic.values[0]
        }) for i in recommended_posts_
    ]

"""
def get_recommended_feed(session: Session, id: int, limit: int) -> List[Post]:
    top_rec = (
        session.query(Post).
        select_from(Feed).
        filter(Feed.action == "like").
        join(Post).
        group_by(Post.id).
        order_by(desc(func.count(Post.id))).
        limit(limit).
        all()
    )
    return top_rec
"""

def get_db() -> Session:
    with SessionLocal() as db:
        return db


@app.get("/user/{id}", response_model=UserGet)
def handle_get_user(id: int, db: Session = Depends(get_db)) -> UserGet:
    user = get_user_by_id(db, id)
    if user is None:
        raise HTTPException(404, "user not found")
    return user


@app.get("/post/{id}", response_model=PostGet)
def handle_get_post(id: int, db: Session = Depends(get_db)) -> PostGet:
    post = get_post_by_id(db, id)
    if post is None:
        raise HTTPException(404, "post not found")
    return post


@app.get("/user/{id}/feed", response_model=List[FeedGet])
def handle_get_feed(id: int, limit: int = 10, db: Session = Depends(get_db)):
    return get_feed(db, id, limit, True)


@app.get("/post/{id}/feed", response_model=List[FeedGet])
def handle_get_feed(id: int, limit: int = 10, db: Session = Depends(get_db)):
    return get_feed(db, id, limit, False)


@app.get("/post/recommendations/", response_model=List[PostGet])
def recommended_posts(id: int, time: datetime, limit: int = 10) -> List[PostGet]:
    return get_recommended_feed(id, time, limit)

"""
def recommended_posts(id: int = None, limit: int = 10, db: Session = Depends(get_db)) -> List[PostGet]:
    rv = get_recommended_feed(db, id, limit)
    return rv
"""





