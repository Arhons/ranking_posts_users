from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import desc
from sqlalchemy.sql import func
from typing import List

from database import SessionLocal
# Для формирования запроса через ORM
from table_user import User
from table_post import Post
from table_feed import Feed
# Для валидации
from schema import UserGet, PostGet, FeedGet

app = FastAPI()

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
def recommended_posts(id: int = None, limit: int = 10, db: Session = Depends(get_db)) -> List[PostGet]:
    rv = get_recommended_feed(db, id, limit)
    return rv





