# -*- coding: utf-8 -*-

"""
What is This
------------------------------------------------------------------------------
This example demonstrates the ultimate strategy to implement one-to-many,
many-to-many relationships in DynamoDB. We will demonstrate this method by
reinventing YouTube in DynamoDB.


Analyze Business Requirement
------------------------------------------------------------------------------
The first step is to list out all the entities involved in your application and how users interact with it.

**Entities**

- 👤 **User**: People must sign up for a Google account to use YouTube. Each Google account represents a user.
- 🎬 **Video**: Users can create and upload videos to the platform.
- 📺 **Channel**: Users can use channels to group their own videos together, helping the author manage access from different paid tiers.
- 💽 **Playlist**: Users can create playlists to organize videos logically, even if the videos are created by other users.

**Relationship**

- **Video Ownership**: User and Video (One-to-Many)
    - Each video is created by only one user (the creator).
    - A single user can publish multiple videos.
- **Channel Ownership**: User and Channel (One-to-Many)
    - Each channel is owned by only one user (the creator).
    - A single user can create and manage multiple channels.
- **Playlist Ownership**: User and Playlist (One-to-Many)
    - Each playlist is created by only one user (the creator).
    - A single user can create and manage multiple playlists.
- **Video-Channel Association**: Video and Channel (Many-to-Many)
    - A single channel can contain multiple videos.
    - A single video can be associated with multiple channels.
- **Video-Playlist Association**: Video and Playlist (Many-to-Many)
    - A single playlist can include multiple videos from different channels and creators.
    - A single video can be added to multiple playlists created by different users.
- **User-User Subscription**: User and User (Many-to-Many)
    - A single user can subscribe to multiple users.
    - A single user can have multiple subscribers (users).
- **User-Channel Subscription**: User and Channel (Many-to-Many)
    - A single user can subscribe to multiple channels.
    - A single channel can have multiple subscribers (users).

**User Interaction**

- People can sign up as a new user.
- User can create a new video.
- User can create a new channel.
- User can create a new playlist.
- User can add video to channel.
- User can add video to playlist.
- User can subscribe another user.
- User can subscribe a channel.

**Query Pattern**

- Given a User id, we can get the detailed information of the user.
- Given a Video id, we can get the detailed information of the video.
- Given a User id, we can get all the videos he created.
- Given a User id, we can get all the channel he created.
- Given a User id, we can get all the playlist he created.
- Given a Channel id, we can get all video in this channel.
- Given a Playlist id, we can get all video in this playlist.
- Given a User id, we can get all youtuber he has subscribed.
- Given a User id, we can get all channel he has subscribed.
- Given a User id, we can get all user who have subscribed him.
- Given a Channel id, we can get all user who have subscribed it.


Install and Import Python Libraries
------------------------------------------------------------------------------
For a POC, we use the following tools to simplify our development:

- [moto](http://docs.getmoto.org/en/latest/index.html): a library that allows you to easily mock out tests based on AWS infrastructure. See [implemented dynamodb feature in moto](http://docs.getmoto.org/en/latest/docs/services/dynamodb.html) for more information.
- [pynamodb_mate](https://pynamodb-mate.readthedocs.io/en/latest/): a powerful DynamoDB SDK python library to implment your ORM DynamoDB model.
- [dataclasses](https://docs.python.org/3/library/dataclasses.html): the Python standard library to implement your application data model.

Run the following command to install dependencies:

```bash
pip install "pynamodb_mate>=5.3.4,<6.0.0"
pip install "moto>=4.2.12,<5.0.0"
```

Terms
------------------------------------------------------------------------------
- Item: a DynamoDB item.
- Entity: a special type of Item, a logical database entity, like user, video, channel, playlist.
- Relationship: a special type of Item, a logical relationship between two entities.
    There are two type of relationship, one to many, many to many. There's no
    one-to-one relationship in DynamoDB because they should be merged into one entity.
"""

import typing as T
import dataclasses

import pynamodb_mate as pm
import pynamodb.exceptions as exc

from moto import mock_dynamodb

# create a DynamoDB connection, ensure that your default AWS credential is right
# if you are using mock, then this line always works
connect = pm.Connection()

# use moto to mock DynamoDB, it is an in-memory implementation of DynamoDB
# you can also use the real DynamoDB table by just comment out the below two line
mock = mock_dynamodb()
mock.start()

# Type hint notation helper
REQUIRED_STR = T.Union[str, pm.UnicodeAttribute]
OPTIONAL_STR = T.Optional[REQUIRED_STR]

# This is a special value for sort key when the item is an entity
ROOT = "--root--"


# ------------------------------------------------------------------------------
# Define DynamoDB Data Model
#
# It is always good to define a class for all your data models. It gives you a centralized place to access all the application logics and query patterns. Also, with proper type hint set up, the compiler will automatically check your typo and data type error for you.
#
# To learn more details about how to write DynamoDB application code in Python, read these:
#
# - `Pynamodb getting started <https://pynamodb.readthedocs.io/en/stable/tutorial.html#getting-started>`_
# - `Pynamodb mate <https://pynamodb-mate.readthedocs.io/en/latest/>`_
#
# **Terms**
#
# - **Item**: a DynamoDB item.
# - **Entity**: a special type of Item, a logical database entity, like user, video, channel, playlist.
# - **Relationship**: a special type of Item, a logical relationship between two entities.
#     There are two type of relationship, one to many, many to many. There's no
#     one-to-one relationship in DynamoDB because they should be merged into one entity.
# ------------------------------------------------------------------------------
class LookupIndex(pm.GlobalSecondaryIndex):
    class Meta:
        index = "lookup-index"
        projection = pm.AllProjection

    sk: REQUIRED_STR = pm.UnicodeAttribute(hash_key=True)


class Entity(pm.Model):
    """
    Main entity item. Base class for all entity items and relationship items.

    :param pk: partition key can only have alpha letter and hyphen.
        For entity item, it is the unique id.
    :param sk: sort key can only have alpha letter and hyphen.
        For entity item, it is always "__root__". For relationship item,
        pk and sk are the two unique ids of the two related entities.
    :param type: item type, can be used to filter by the type.
    :param name: human friendly name of the entity.
    """

    class Meta:
        table_name = "entity"
        region = "us-east-1"
        billing_mode = pm.PAY_PER_REQUEST_BILLING_MODE

    # partition key and sort key
    pk: REQUIRED_STR = pm.UnicodeAttribute(hash_key=True)
    sk: REQUIRED_STR = pm.UnicodeAttribute(range_key=True)

    # common attributes
    type: REQUIRED_STR = pm.UnicodeAttribute()
    name: OPTIONAL_STR = pm.UnicodeAttribute(null=True)

    lookup_index = LookupIndex()

    def print(self):
        """
        Print necessary attributes of an entity item, only include human friendly attributes.
        """
        d = dict(
            type=self.type,
            pk=self.pk,
            sk=self.sk,
        )
        if self.name:
            d["name"] = self.name
        print(d)

    @property
    def pk_id(self):
        return self.pk.split("_")[0]

    @property
    def sk_id(self):
        return self.sk.split("_")[0]


Entity.create_table(wait=True)


class User(Entity):
    lookup_index = LookupIndex()

    @property
    def user_id(self) -> str:
        return self.pk_id


class Video(Entity):
    lookup_index = LookupIndex()

    @property
    def video_id(self) -> str:
        return self.pk_id


class Channel(Entity):
    lookup_index = LookupIndex()

    @property
    def channel_id(self) -> str:
        return self.pk_id


class Playlist(Entity):
    lookup_index = LookupIndex()

    @property
    def playlist_id(self) -> str:
        return self.pk_id


class VideoOwnership(Entity):
    lookup_index = LookupIndex()

    @property
    def video_id(self) -> str:
        return self.pk_id

    @property
    def user_id(self) -> str:
        return self.sk_id


class ChannelOwnership(Entity):
    lookup_index = LookupIndex()

    @property
    def channel_id(self) -> str:
        return self.pk_id

    @property
    def user_id(self) -> str:
        return self.sk_id


class PlaylistOwnership(Entity):
    lookup_index = LookupIndex()

    @property
    def playlist_id(self) -> str:
        return self.pk_id

    @property
    def user_id(self) -> str:
        return self.sk_id


class VideoChannelAssociation(Entity):
    lookup_index = LookupIndex()

    @property
    def video_id(self) -> str:
        return self.pk_id

    @property
    def channel_id(self) -> str:
        return self.sk_id


class VideoPlaylistAssociation(Entity):
    lookup_index = LookupIndex()

    @property
    def video_id(self) -> str:
        return self.pk_id

    @property
    def playlist_id(self) -> str:
        return self.sk_id


class ViewerSubscribeYoutuber(Entity):
    lookup_index = LookupIndex()

    @property
    def viewer_id(self) -> str:
        return self.pk_id

    @property
    def youtuber_id(self) -> str:
        return self.sk_id


class ViewerSubscribeChannel(Entity):
    lookup_index = LookupIndex()

    @property
    def viewer_id(self) -> str:
        return self.pk_id

    @property
    def channel_id(self) -> str:
        return self.sk_id


T_Entity = T.Union[
    Entity,
    User,
    Video,
    Channel,
    Playlist,
    VideoOwnership,
    ChannelOwnership,
    PlaylistOwnership,
    VideoChannelAssociation,
    VideoPlaylistAssociation,
    ViewerSubscribeYoutuber,
    ViewerSubscribeChannel,
]
T_Entity_Type = T.Union[
    T.Type[Entity],
    T.Type[User],
    T.Type[Video],
    T.Type[Channel],
    T.Type[Playlist],
    T.Type[VideoOwnership],
    T.Type[ChannelOwnership],
    T.Type[PlaylistOwnership],
    T.Type[VideoChannelAssociation],
    T.Type[VideoPlaylistAssociation],
    T.Type[ViewerSubscribeYoutuber],
    T.Type[ViewerSubscribeChannel],
]


# ------------------------------------------------------------------------------
# Define Relationship Metadata
#
# DynamoDB ORM Model doesn't know the relationship between entities. We need to
# define the relationship metadata to help us to implement the business logic.
# ------------------------------------------------------------------------------
class TypeEnum:
    entity = "entity"
    o2m = "o2m"
    m2m = "m2m"


@dataclasses.dataclass
class ItemType:
    """
    :param name: unique type name of the entity or relationship.
    :param type: entity | o2m (one to many) | m2m (many to many)
    :param klass: DynamoDB ORM class of the entity or relationship.
    :param one_klass: for one-to-many relationship only.
        The DynamoDB ORM class of the one entity.
    :param many_klass: for one-to-many relationship only.
        The DynamoDB ORM class of the many entity.
    :param left_klass: for many-to-many relationship only.
        The DynamoDB ORM class of the left entity.
    :param right_klass: for many-to-many relationship only.
        The DynamoDB ORM class of the right entity.
    """

    name: str = dataclasses.field()
    type: str = dataclasses.field()
    klass: T.Type[T_Entity] = dataclasses.field()
    one_klass: T.Type[T_Entity] = dataclasses.field(default=None)
    many_klass: T.Type[T_Entity] = dataclasses.field(default=None)
    left_klass: T.Optional[T.Type[T_Entity]] = dataclasses.field(default=None)
    right_klass: T.Optional[T.Type[T_Entity]] = dataclasses.field(default=None)


user_type = ItemType(
    name="USER",
    type=TypeEnum.entity,
    klass=User,
)
video_type = ItemType(
    name="VIDEO",
    type=TypeEnum.entity,
    klass=Video,
)
channel_type = ItemType(
    name="CHANNEL",
    type=TypeEnum.entity,
    klass=Channel,
)
playlist_type = ItemType(
    name="PLAYLIST",
    type=TypeEnum.entity,
    klass=Playlist,
)
video_ownership_type = ItemType(
    name="VIDEO-OWNERSHIP",
    type=TypeEnum.o2m,
    klass=VideoOwnership,
    one_klass=User,
    many_klass=Video,
)
channel_ownership_type = ItemType(
    name="CHANNEL-OWNERSHIP",
    type=TypeEnum.o2m,
    klass=ChannelOwnership,
    one_klass=User,
    many_klass=Channel,
)
playlist_ownership_type = ItemType(
    name="PLAYLIST-OWNERSHIP",
    type=TypeEnum.o2m,
    klass=PlaylistOwnership,
    one_klass=User,
    many_klass=Playlist,
)
video_channel_association_type = ItemType(
    name="VIDEO-CHANNEL-ASSOCIATION",
    type=TypeEnum.m2m,
    klass=VideoChannelAssociation,
    left_klass=Video,
    right_klass=Channel,
)
video_playlist_association_type = ItemType(
    name="VIDEO-PLAYLIST-ASSOCIATION",
    type=TypeEnum.m2m,
    klass=VideoPlaylistAssociation,
    left_klass=Video,
    right_klass=Playlist,
)
viewer_subscribe_youtuber_type = ItemType(
    name="VIEWER-SUBSCRIBE-YOUTUBER",
    type=TypeEnum.m2m,
    klass=ViewerSubscribeYoutuber,
    left_klass=User,
    right_klass=User,
)
viewer_subscribe_channel_type = ItemType(
    name="VIEWER-SUBSCRIBE-CHANNEL",
    type=TypeEnum.m2m,
    klass=ViewerSubscribeChannel,
    left_klass=User,
    right_klass=Channel,
)

item_type_list = [
    user_type,
    video_type,
    channel_type,
    playlist_type,
    video_ownership_type,
    channel_ownership_type,
    playlist_ownership_type,
    video_channel_association_type,
    video_playlist_association_type,
    viewer_subscribe_youtuber_type,
    viewer_subscribe_channel_type,
]


# ------------------------------------------------------------------------------
# Implement Business Operations
# ------------------------------------------------------------------------------
@dataclasses.dataclass
class BusinessOperation:
    """
    Business Operation as a method
    """

    item_type_list: list[ItemType] = dataclasses.field()
    _item_type_mapper: dict[str, ItemType] = dataclasses.field(init=False)

    def __post_init__(self):
        self._item_type_mapper = {
            item_type.name: item_type for item_type in self.item_type_list
        }
        if len(self._item_type_mapper) != len(self.item_type_list):
            raise ValueError("item_type_list has duplicate name")

    def _get_type(self, name: str) -> ItemType:
        return self._item_type_mapper[name]

    def new_entity(
        self,
        type: ItemType,
        id: str,
        name: str,
        save: bool = True,
    ) -> T.Optional[Entity]:
        if type.type != TypeEnum.entity:
            raise ValueError(f"Type {type.name} is not entity")
        klass = type.klass
        entity = klass(
            pk=id,
            sk=ROOT,
            type=type.name,
            name=name,
        )
        if save is False:
            return entity
        try:
            # ensure that the entity does not exist
            res = entity.save(
                condition=(~klass.pk.exists()),
            )
            return entity
        except exc.PutError as e:
            return None

    def new_user(
        self,
        id: str,
        name: str,
        save: bool = True,
    ) -> T.Optional[User]:
        return self.new_entity(user_type, id, name, save)

    def new_video(
        self,
        id: str,
        name: str,
        save: bool = True,
    ) -> T.Optional[Video]:
        return self.new_entity(video_type, id, name, save)

    def new_channel(
        self,
        id: str,
        name: str,
        save: bool = True,
    ) -> T.Optional[Channel]:
        return self.new_entity(video_type, id, name, save)

    def new_playlist(
        self,
        id: str,
        name: str,
        save: bool = True,
    ) -> T.Optional[Playlist]:
        return self.new_entity(playlist_type, id, name, save)

    def scan(self) -> list[T_Entity]:
        return list(Entity.scan())

    def scan_and_print(self):
        for entity in self.scan():
            entity.print()

    def list_entities(self, type: ItemType) -> list[T_Entity]:
        klass = type.klass
        return list(klass.scan(filter_condition=(klass.type == type.name)))

    def list_users(self) -> list[User]:
        return self.list_entities(user_type)

    def list_videos(self) -> list[User]:
        return self.list_entities(video_type)

    def list_channels(self) -> list[User]:
        return self.list_entities(channel_type)

    def list_playlists(self) -> list[User]:
        return self.list_entities(playlist_type)

    def set_one_to_many(
        self,
        type: ItemType,
        many_entity_id: str,
        one_entity_id: str,
    ):
        """
        For example, one user has many videos, one video only belongs to one user.
        Then, this function is used to set ownership of a video. In this case,
        the video is the many entity, the user is the one entity.
        """
        if type.type != TypeEnum.o2m:
            raise ValueError(f"Type {type.name} is not one-to-many relationship")
        klass = type.klass

        with pm.TransactWrite(
            connection=connect,
            client_request_token=f"{many_entity_id}_{one_entity_id}_{type}",
        ) as trans:
            # find all existing relationship entities and delete them
            # it won't have
            r_entities = list(
                klass.query(
                    hash_key=f"{many_entity_id}_{type.name}",
                )
            )
            for r_entity in r_entities:
                trans.delete(r_entity)
            # create a new relationship entity
            r_entity = klass(
                pk=f"{many_entity_id}_{type.name}",
                sk=f"{one_entity_id}_{type.name}",
                type=type.name,
            )
            trans.save(r_entity)

    def set_video_owner(
        self,
        video_id: str,
        user_id: str,
    ):
        self.set_one_to_many(video_ownership_type, video_id, user_id)

    def set_channel_owner(
        self,
        channel_id: str,
        user_id: str,
    ):
        self.set_one_to_many(channel_ownership_type, channel_id, user_id)

    def set_playlist_owner(
        self,
        playlist_id: str,
        user_id: str,
    ):
        self.set_one_to_many(playlist_ownership_type, playlist_id, user_id)

    def find_many_by_one(
        self,
        type: ItemType,
        one_entity_id: str,
    ) -> list[T_Entity]:
        if type.type != TypeEnum.o2m:
            raise ValueError(f"Type {type.name} is not one-to-many relationship")
        klass = type.klass
        result = klass.lookup_index.query(
            hash_key=f"{one_entity_id}_{type.name}",
        )
        return list(result)

    def find_videos_created_by_a_user(self, user_id: str) -> T.List[Video]:
        return self.find_many_by_one(video_ownership_type, user_id)

    def find_channels_created_by_a_user(self, user_id: str) -> T.List[Channel]:
        return self.find_many_by_one(channel_ownership_type, user_id)

    def find_playlists_created_by_a_user(self, user_id: str) -> T.List[Playlist]:
        return self.find_many_by_one(playlist_ownership_type, user_id)

    def set_many_to_many(
        self,
        type: ItemType,
        left_entity_id: str,
        right_entity_id: str,
    ):
        klass = type.klass
        klass(
            pk=f"{left_entity_id}_{type.name}",
            sk=f"{right_entity_id}_{type.name}",
            type=type.name,
        ).save(
            condition=~(klass.pk.exists() & klass.sk.exists()),
        )

    def add_video_to_channel(
        self,
        video_id: str,
        channel_id: str,
    ):
        self.set_many_to_many(
            type=video_channel_association_type,
            left_entity_id=video_id,
            right_entity_id=channel_id,
        )

    def add_video_to_playlist(
        self,
        video_id: str,
        playlist_id: str,
    ):
        self.set_many_to_many(
            type=video_playlist_association_type,
            left_entity_id=video_id,
            right_entity_id=playlist_id,
        )

    def subscribe_user(
        self,
        viewer_id: str,
        youtuber_id: str,
    ):
        self.set_many_to_many(
            type=viewer_subscribe_youtuber_type,
            left_entity_id=viewer_id,
            right_entity_id=youtuber_id,
        )

    def subscribe_channel(
        self,
        viewer_id: str,
        channel_id: str,
    ):
        self.set_many_to_many(
            type=viewer_subscribe_channel_type,
            left_entity_id=viewer_id,
            right_entity_id=channel_id,
        )

    def find_many_by_many(
        self,
        type: ItemType,
        entity_id: str,
        lookup_by_left: bool,
    ) -> list[T_Entity]:
        klass = type.klass
        if lookup_by_left:
            return list(
                klass.query(
                    hash_key=f"{entity_id}_{type.name}",
                )
            )
        else:
            return list(
                klass.lookup_index.query(
                    hash_key=f"{entity_id}_{type.name}",
                )
            )

    def find_videos_in_channel(
        self,
        channel_id: str,
    ) -> T.List[VideoChannelAssociation]:
        return self.find_many_by_many(
            type=video_channel_association_type,
            entity_id=channel_id,
            lookup_by_left=False,
        )

    def find_channels_that_has_video(
        self,
        video_id: str,
    ) -> T.List[VideoChannelAssociation]:
        return self.find_many_by_many(
            type=video_channel_association_type,
            entity_id=video_id,
            lookup_by_left=True,
        )

    def find_videos_in_playlist(
        self,
        playlist_id: str,
    ) -> T.List[VideoPlaylistAssociation]:
        return self.find_many_by_many(
            type=video_playlist_association_type,
            entity_id=playlist_id,
            lookup_by_left=False,
        )

    def find_playlists_that_has_video(
        self,
        video_id: str,
    ) -> T.List[VideoPlaylistAssociation]:
        return self.find_many_by_many(
            type=video_playlist_association_type,
            entity_id=video_id,
            lookup_by_left=True,
        )

    def find_followers_for_user(
        self,
        user_id: str,
    ) -> T.List[ViewerSubscribeYoutuber]:
        return self.find_many_by_many(
            type=viewer_subscribe_youtuber_type,
            entity_id=user_id,
            lookup_by_left=False,
        )

    def find_subscribed_youtubers(
        self,
        user_id: str,
    ) -> T.List[ViewerSubscribeYoutuber]:
        return self.find_many_by_many(
            type=viewer_subscribe_youtuber_type,
            entity_id=user_id,
            lookup_by_left=True,
        )

    def find_followers_for_channel(
        self,
        channel_id: str,
    ) -> T.List[ViewerSubscribeChannel]:
        return self.find_many_by_many(
            type=viewer_subscribe_channel_type,
            entity_id=channel_id,
            lookup_by_left=False,
        )

    def find_subscribed_channels(
        self,
        user_id: str,
    ) -> T.List[ViewerSubscribeChannel]:
        return self.find_many_by_many(
            type=viewer_subscribe_channel_type,
            entity_id=user_id,
            lookup_by_left=True,
        )


op = BusinessOperation(item_type_list=item_type_list)

# ------------------------------------------------------------------------------
# Setup Dummy Data For Testing
# ------------------------------------------------------------------------------
# Create users
u_alice = op.new_user(id="u-1", name="Alice")
u_bob = op.new_user(id="u-2", name="Bob")
u_cathy = op.new_user(id="u-3", name="Cathy")
u_david = op.new_user(id="u-4", name="David")

# Create videos
v_alice_1 = op.new_video(id="v-1-1", name="Alice's Video 1")
op.set_video_owner(video_id="v-1-1", user_id="u-1")
v_alice_2 = op.new_video(id="v-1-2", name="Alice's Video 2")
op.set_video_owner(video_id="v-1-2", user_id="u-1")

v_bob_1 = op.new_video(id="v-2-1", name="Bob's Video 1")
op.set_video_owner(video_id="v-2-1", user_id="u-2")
v_bob_2 = op.new_video(id="v-2-2", name="Bob's Video 2")
op.set_video_owner(video_id="v-2-2", user_id="u-2")
v_bob_3 = op.new_video(id="v-2-3", name="Bob's Video 3")
op.set_video_owner(video_id="v-2-3", user_id="u-2")
v_bob_4 = op.new_video(id="v-2-4", name="Bob's Video 4")
op.set_video_owner(video_id="v-2-4", user_id="u-2")

# Create channels
c_alice_1 = op.new_channel(id="c-1-1", name="Alice's Channel 1")
op.set_channel_owner(channel_id="c-1-1", user_id="u-1")
c_bob_1 = op.new_channel(id="c-2-1", name="Bob's Channel 1")
op.set_channel_owner(channel_id="c-2-1", user_id="u-2")
c_bob_2 = op.new_channel(id="c-2-2", name="Bob's Channel 2")
op.set_channel_owner(channel_id="c-2-2", user_id="u-2")

# Create playlists
p_cathy_1 = op.new_playlist(id="p-3-1", name="Cathy's Playlist 1")
op.set_playlist_owner(playlist_id="p-3-1", user_id="u-3")
p_cathy_2 = op.new_playlist(id="p-3-2", name="Cathy's Playlist 2")
op.set_playlist_owner(playlist_id="p-3-2", user_id="u-3")

# Create video and channel association
op.add_video_to_channel(video_id="v-2-1", channel_id="c-2-1")
op.add_video_to_channel(video_id="v-2-2", channel_id="c-2-1")
op.add_video_to_channel(video_id="v-2-3", channel_id="c-2-1")

op.add_video_to_channel(video_id="v-2-2", channel_id="c-2-2")
op.add_video_to_channel(video_id="v-2-3", channel_id="c-2-2")
op.add_video_to_channel(video_id="v-2-4", channel_id="c-2-2")

# Create video and playlist association
op.add_video_to_playlist(video_id="v-2-1", playlist_id="p-3-1")
op.add_video_to_playlist(video_id="v-2-2", playlist_id="p-3-1")
op.add_video_to_playlist(video_id="v-2-3", playlist_id="p-3-1")

op.add_video_to_playlist(video_id="v-2-2", playlist_id="p-3-2")
op.add_video_to_playlist(video_id="v-2-3", playlist_id="p-3-2")
op.add_video_to_playlist(video_id="v-2-4", playlist_id="p-3-2")

# Create viewer and youtuber subscription
op.subscribe_user(viewer_id="u-1", youtuber_id="u-2")
op.subscribe_user(viewer_id="u-2", youtuber_id="u-1")
op.subscribe_user(viewer_id="u-3", youtuber_id="u-1")
op.subscribe_user(viewer_id="u-3", youtuber_id="u-2")
op.subscribe_user(viewer_id="u-4", youtuber_id="u-1")
op.subscribe_user(viewer_id="u-4", youtuber_id="u-3")

# Create viewer and channel subscription
op.subscribe_channel(viewer_id="u-1", channel_id="c-2-1")
op.subscribe_channel(viewer_id="u-1", channel_id="c-2-2")
op.subscribe_channel(viewer_id="u-2", channel_id="c-1-1")
op.subscribe_channel(viewer_id="u-3", channel_id="c-1-1")
op.subscribe_channel(viewer_id="u-3", channel_id="c-2-1")
op.subscribe_channel(viewer_id="u-4", channel_id="c-2-2")


# ------------------------------------------------------------------------------
# Test Business Operations
# ------------------------------------------------------------------------------
# declare some helpers
def assert_pk(lst: T.Iterable[T_Entity], pks: T.List[str]):
    """
    A helper function to verify a list of items' partition key.
    """
    assert set(x.pk_id for x in lst) == set(pks)


def assert_sk(lst: T.Iterable[T_Entity], sks: T.List[str]):
    """
    A helper function to verify a list of items' sort key. Usually used
    for lookup in one-to-many and many-to-many relationship.
    """
    assert set(x.sk_id for x in lst) == set(sks)


def print_all(lst: T.Iterable[T_Entity]):
    for entity in lst:
        entity.print()


print("--- Scan entities and relationships ---")
res = op.scan()
print_all(res)

print("--- Alice owned videos ---")
res = op.find_videos_created_by_a_user(user_id="u-1")
print_all(res)
assert_pk(res, ["v-1-1", "v-1-2"])

print("--- Bob owned videos ---")
res = op.find_videos_created_by_a_user(user_id="u-2")
print_all(res)
assert_pk(res, ["v-2-1", "v-2-2", "v-2-3", "v-2-4"])

print("--- Alice owned channels ---")
res = op.find_channels_created_by_a_user(user_id="u-1")
print_all(res)
assert_pk(res, ["c-1-1"])

print("--- Bob owned channels ---")
res = op.find_channels_created_by_a_user(user_id="u-2")
print_all(res)
assert_pk(res, ["c-2-1", "c-2-2"])

print("--- Cathy owned playlists ---")
res = op.find_playlists_created_by_a_user(user_id="u-3")
print_all(res)
assert_pk(res, ["p-3-1", "p-3-2"])

print("--- Videos in Bob's Channel 1 ---")
res = op.find_videos_in_channel(channel_id="c-2-1")
print_all(res)
assert_pk(res, ["v-2-1", "v-2-2", "v-2-3"])

print("--- Videos in Bob's Channel 2 ---")
res = op.find_videos_in_channel(channel_id="c-2-2")
print_all(res)
assert_pk(res, ["v-2-2", "v-2-3", "v-2-4"])

print("--- Channels that has Bob's Video 1 ---")
res = op.find_channels_that_has_video(video_id="v-2-1")
print_all(res)
assert_sk(res, ["c-2-1"])

print("--- Channels that has Bob's Video 2 ---")
res = op.find_channels_that_has_video(video_id="v-2-2")
print_all(res)
assert_sk(res, ["c-2-1", "c-2-2"])

print("--- Channels that has Bob's Video 3 ---")
res = op.find_channels_that_has_video(video_id="v-2-3")
print_all(res)
assert_sk(res, ["c-2-1", "c-2-2"])

print("--- Channels that has Bob's Video 4 ---")
res = op.find_channels_that_has_video(video_id="v-2-4")
print_all(res)
assert_sk(res, ["c-2-2"])

print("--- Videos in Cathy's Playlist 1 ---")
res = op.find_videos_in_playlist(playlist_id="p-3-1")
print_all(res)
assert_pk(res, ["v-2-1", "v-2-2", "v-2-3"])

print("--- Videos in Cathy's Playlist 2 ---")
res = op.find_videos_in_playlist(playlist_id="p-3-2")
print_all(res)
assert_pk(res, ["v-2-2", "v-2-3", "v-2-4"])

print("--- Playlist that has Bob's Video 1 ---")
res = op.find_playlists_that_has_video(video_id="v-2-1")
print_all(res)
assert_sk(res, ["p-3-1"])

print("--- Playlist that has Bob's Video 2 ---")
res = op.find_playlists_that_has_video(video_id="v-2-2")
print_all(res)
assert_sk(res, ["p-3-1", "p-3-2"])

print("--- Playlist that has Bob's Video 3 ---")
res = op.find_playlists_that_has_video(video_id="v-2-3")
print_all(res)
assert_sk(res, ["p-3-1", "p-3-2"])

print("--- Playlist that has Bob's Video 4 ---")
res = op.find_playlists_that_has_video(video_id="v-2-4")
print_all(res)
assert_sk(res, ["p-3-2"])

print("--- Users who subscribes Alice ---")
res = op.find_followers_for_user(user_id="u-1")
print_all(res)
assert_pk(res, ["u-2", "u-3", "u-4"])

print("--- Users who subscribes Bob ---")
res = op.find_followers_for_user(user_id="u-2")
print_all(res)
assert_pk(res, ["u-1", "u-3"])

print("--- Users who subscribes Cathy ---")
res = op.find_followers_for_user(user_id="u-3")
print_all(res)
assert_pk(res, ["u-4"])

print("--- Users who subscribes David ---")
res = op.find_followers_for_user(user_id="u-4")
print_all(res)
assert_pk(res, [])

print("--- Alice subscribed who ---")
res = op.find_subscribed_youtubers(user_id="u-1")
print_all(res)
assert_sk(res, ["u-2"])

print("--- Bob subscribed who ---")
res = op.find_subscribed_youtubers(user_id="u-2")
print_all(res)
assert_sk(res, ["u-1"])

print("--- Cathy subscribed who ---")
res = op.find_subscribed_youtubers(user_id="u-3")
print_all(res)
assert_sk(res, ["u-1", "u-2"])

print("--- David subscribed who ---")
res = op.find_subscribed_youtubers(user_id="u-4")
print_all(res)
assert_sk(res, ["u-1", "u-3"])

print("--- Users who subscribes Alice' Channel 1 ---")
res = op.find_followers_for_channel(channel_id="c-1-1")
print_all(res)
assert_pk(res, ["u-2", "u-3"])

print("--- Users who subscribes Bob' Channel 1 ---")
res = op.find_followers_for_channel(channel_id="c-2-1")
print_all(res)
assert_pk(res, ["u-1", "u-3"])

print("--- Users who subscribes Bob' Channel 2 ---")
res = op.find_followers_for_channel(channel_id="c-2-2")
print_all(res)
assert_pk(res, ["u-1", "u-4"])

print("--- Alice subscribed channels ---")
res = op.find_subscribed_channels(user_id="u-1")
print_all(res)
assert_sk(res, ["c-2-1", "c-2-2"])

print("--- Bob subscribed channels ---")
res = op.find_subscribed_channels(user_id="u-2")
print_all(res)
assert_sk(res, ["c-1-1"])

print("--- Cathy subscribed channels ---")
res = op.find_subscribed_channels(user_id="u-3")
print_all(res)
assert_sk(res, ["c-1-1", "c-2-1"])

print("--- David subscribed channels ---")
res = op.find_subscribed_channels(user_id="u-4")
print_all(res)
assert_sk(res, ["c-2-2"])
