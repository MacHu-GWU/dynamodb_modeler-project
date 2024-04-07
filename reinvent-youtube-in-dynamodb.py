# -*- coding: utf-8 -*-

import typing as T
from datetime import datetime, timezone

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

REQUIRED_STR = T.Union[str, pm.UnicodeAttribute]
OPTIONAL_STR = T.Optional[REQUIRED_STR]
REQUIRED_INT = T.Union[int, pm.NumberAttribute]
OPTIONAL_INT = T.Optional[REQUIRED_INT]
REQUIRED_DATETIME = T.Union[datetime, pm.UTCDateTimeAttribute]
OPTIONAL_DATETIME = T.Optional[REQUIRED_DATETIME]


ROOT = "__root__"


class LookupIndex(pm.GlobalSecondaryIndex):
    class Meta:
        index = "lookup-index"
        projection = pm.AllProjection

    sk: REQUIRED_STR = pm.UnicodeAttribute(hash_key=True)


def get_utc_now() -> datetime:
    return datetime.utcnow().replace(tzinfo=timezone.utc)


class Entity(pm.Model):
    """
    :param pk: partition key can only has alpha letter and hyphen.
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
    # create_at: REQUIRED_DATETIME = pm.UTCDateTimeAttribute(null=False)
    # update_at: REQUIRED_DATETIME = pm.UTCDateTimeAttribute(null=False)

    lookup_index = LookupIndex()

    def print(self):
        print(
            dict(
                type=self.type,
                pk=self.pk,
                sk=self.sk,
                name=self.name,
            )
        )


T_Entity = T.TypeVar("T_Entity", bound=Entity)

Entity.create_table(wait=True)


class User(Entity):
    lookup_index = LookupIndex()

    @property
    def user_id(self) -> str:
        return self.pk


class Video(Entity):
    lookup_index = LookupIndex()

    @property
    def video_id(self) -> str:
        return self.pk


class Channel(Entity):
    lookup_index = LookupIndex()

    @property
    def channel_id(self) -> str:
        return self.pk


class Playlist(Entity):
    lookup_index = LookupIndex()

    @property
    def playlist_id(self) -> str:
        return self.pk


class VideoOwnership(Entity):
    lookup_index = LookupIndex()

    @property
    def video_id(self) -> str:
        return self.pk

    @property
    def user_id(self) -> str:
        return self.sk.split("_")[0]


class ChannelOwnership(Entity):
    lookup_index = LookupIndex()

    @property
    def channel_id(self) -> str:
        return self.pk

    @property
    def user_id(self) -> str:
        return self.sk.split("_")[0]


class PlaylistOwnership(Entity):
    lookup_index = LookupIndex()

    @property
    def playlist_id(self) -> str:
        return self.pk

    @property
    def user_id(self) -> str:
        return self.sk.split("_")[0]


class VideoChannelAssociation(Entity):
    lookup_index = LookupIndex()


class VideoPlaylistAssociation(Entity):
    lookup_index = LookupIndex()


class UserSubscribeUser(Entity):
    lookup_index = LookupIndex()


class UserSubscribeChannel(Entity):
    lookup_index = LookupIndex()


class TypeEnum:
    """
    type can only has alpha letter and hyphen.
    """

    # entities
    USER = "USER"
    VIDEO = "VIDEO"
    CHANNEL = "CHANNEL"
    PLAYLIST = "PLAYLIST"

    # relationships
    VIDEO_OWNERSHIP = "VIDEO-OWNERSHIP"
    CHANNEL_OWNERSHIP = "CHANNEL-OWNERSHIP"
    PLAYLIST_OWNERSHIP = "PLAYLIST-OWNERSHIP"
    VIDEO_CHANNEL_ASSOCIATION = "VIDEO-CHANNEL-ASSOCIATION"
    VIDEO_PLAYLIST_ASSOCIATION = "VIDEO-PLAYLIST-ASSOCIATION"
    USER_SUBSCRIBE_USER = "USER-SUBSCRIBE-USER"
    USER_SUBSCRIBE_CHANNEL = "USER-SUBSCRIBE-CHANNEL"


type_to_klass_mapper: dict[str, T.Type[T_Entity]] = {
    TypeEnum.USER: User,
    TypeEnum.VIDEO: Video,
    TypeEnum.CHANNEL: Channel,
    TypeEnum.PLAYLIST: Playlist,
    TypeEnum.VIDEO_OWNERSHIP: VideoOwnership,
    TypeEnum.CHANNEL_OWNERSHIP: ChannelOwnership,
    TypeEnum.PLAYLIST_OWNERSHIP: PlaylistOwnership,
    TypeEnum.VIDEO_CHANNEL_ASSOCIATION: VideoChannelAssociation,
    TypeEnum.VIDEO_PLAYLIST_ASSOCIATION: VideoPlaylistAssociation,
    TypeEnum.USER_SUBSCRIBE_USER: UserSubscribeUser,
    TypeEnum.USER_SUBSCRIBE_CHANNEL: UserSubscribeChannel,
}

relationship_to_klass_mapper: dict[
    str,
    tuple[
        T.Type[T_Entity],
        T.Type[T_Entity],
    ],
] = {
    TypeEnum.VIDEO_OWNERSHIP: (User, Video),
    TypeEnum.CHANNEL_OWNERSHIP: (User, Channel),
    TypeEnum.PLAYLIST_OWNERSHIP: (User, Playlist),
    TypeEnum.VIDEO_CHANNEL_ASSOCIATION: (Video, Channel),
    TypeEnum.VIDEO_PLAYLIST_ASSOCIATION: (Video, Channel),
    TypeEnum.USER_SUBSCRIBE_USER: (User, User),
    TypeEnum.USER_SUBSCRIBE_CHANNEL: (User, Channel),
}


class OP:
    def new_entity(
        self,
        type: str,
        id: str,
        name: str,
        save: bool = True,
    ) -> T.Optional[Entity]:
        now = get_utc_now()
        klass = type_to_klass_mapper[type]
        entity = klass(
            pk=id,
            sk=ROOT,
            type=type,
            name=name,
            # create_at=now,
            # update_at=now,
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

    def new_user(self, id: str, name: str) -> T.Optional[User]:
        return self.new_entity(TypeEnum.USER, id, name)

    def new_video(self, id: str, name: str, user_id: str) -> T.Optional[Video]:
        video = self.new_entity(TypeEnum.VIDEO, id, name)
        if video is None:
            return None
        self.set_one_to_many(
            type=TypeEnum.VIDEO_OWNERSHIP, many_entity_id=id, one_entity_id=user_id
        )
        return video

    def new_channel(self, id: str, name: str, user_id: str) -> T.Optional[Channel]:
        channel = self.new_entity(TypeEnum.CHANNEL, id, name)
        if channel is None:
            return None
        self.set_one_to_many(
            type=TypeEnum.CHANNEL_OWNERSHIP, many_entity_id=id, one_entity_id=user_id
        )
        return channel

    def new_playlist(self, id: str, name: str, user_id: str) -> T.Optional[Playlist]:
        playlist = self.new_entity(TypeEnum.PLAYLIST, id, name)
        if playlist is None:
            return None
        self.set_one_to_many(
            type=TypeEnum.PLAYLIST_OWNERSHIP, many_entity_id=id, one_entity_id=user_id
        )
        return playlist

    def scan(self) -> list[T_Entity]:
        return list(Entity.scan())

    def list_entities(self, type: str) -> list[T_Entity]:
        klass = type_to_klass_mapper[type]
        return list(klass.scan(filter_condition=(klass.type == type)))

    def list_users(self) -> list[User]:
        return self.list_entities(TypeEnum.USER)

    def list_videos(self) -> list[User]:
        return self.list_entities(TypeEnum.VIDEO)

    def list_channels(self) -> list[User]:
        return self.list_entities(TypeEnum.CHANNEL)

    def list_playlists(self) -> list[User]:
        return self.list_entities(TypeEnum.PLAYLIST)

    def set_one_to_many(
        self,
        type: str,
        many_entity_id: str,
        one_entity_id: str,
    ):
        r_klass = type_to_klass_mapper[type]
        one_klass, many_klass = relationship_to_klass_mapper[type]

        with pm.TransactWrite(
            connection=connect,
            client_request_token=f"{many_entity_id}-{one_entity_id}-{type}",
        ) as trans:
            # find all existing relationship entities and delete them
            r_entities = list(
                r_klass.query(
                    hash_key=many_entity_id, filter_condition=(r_klass.type == type)
                )
            )
            for r_entity in r_entities:
                trans.delete(r_entity)
            # create a new relationship entity
            now = get_utc_now()
            r_entity = many_klass(
                pk=many_entity_id,
                sk=f"{one_entity_id}_{type}",
                type=type,
                # create_at=now,
                # update_at=now,
            )
            trans.save(r_entity)

    def find_many_by_one(
        self,
        type: str,
        one_entity_id: str,
    ) -> list[T_Entity]:
        one_klass, many_klass = relationship_to_klass_mapper[type]
        result = many_klass.lookup_index.query(
            hash_key=f"{one_entity_id}_{type}",
            filter_condition=(many_klass.type == type),
        )
        return list(result)

    def find_videos_created_by_a_user(self, user_id: str) -> T.List[Video]:
        return self.find_many_by_one(TypeEnum.VIDEO_OWNERSHIP, user_id)

    def find_channels_created_by_a_user(self, user_id: str) -> T.List[Channel]:
        return self.find_many_by_one(TypeEnum.CHANNEL_OWNERSHIP, user_id)

    def find_playlists_created_by_a_user(self, user_id: str) -> T.List[Playlist]:
        return self.find_many_by_one(TypeEnum.PLAYLIST_OWNERSHIP, user_id)

    def set_many_to_many(
        self,
        type: str,
        left_entity_id: str,
        right_entity_id: str,
    ):
        r_klass = type_to_klass_mapper[type]
        r_klass(
            pk=left_entity_id,
            sk=f"{right_entity_id}-{type}",
            type=type,
        ).save(
            condition=~(r_klass.pk.exists() & r_klass.sk.exists()),
        )

    def add_video_to_channel(
        self,
        video_id: str,
        channel_id: str,
    ):
        self.set_many_to_many(
            type=TypeEnum.VIDEO_CHANNEL_ASSOCIATION,
            left_entity_id=video_id,
            right_entity_id=channel_id,
        )

    def add_video_to_playlist(
        self,
        video_id: str,
        playlist_id: str,
    ):
        self.set_many_to_many(
            type=TypeEnum.VIDEO_PLAYLIST_ASSOCIATION,
            left_entity_id=video_id,
            right_entity_id=playlist_id,
        )

    def subscribe_user(
        self,
        subscriber_id: str,
        publisher_id: str,
    ):
        self.set_many_to_many(
            type=TypeEnum.USER_SUBSCRIBE_USER,
            left_entity_id=subscriber_id,
            right_entity_id=publisher_id,
        )

    def subscribe_channel(
        self,
        subscriber_id: str,
        channel_id: str,
    ):
        self.set_many_to_many(
            type=TypeEnum.USER_SUBSCRIBE_CHANNEL,
            left_entity_id=subscriber_id,
            right_entity_id=channel_id,
        )

    def find_many_by_many(
        self,
        type: str,
        entity_id: str,
        lookup_by_left: bool,
    ) -> list[T_Entity]:
        r_klass = type_to_klass_mapper[type]
        # left_entity_klass, right_entity_klass = relationship_to_klass_mapper[type]
        if lookup_by_left:
            return list(
                r_klass.query(
                    hash_key=entity_id,
                    filter_condition=Entity.type == type,
                )
            )
        else:
            return list(
                r_klass.lookup_index.query(
                    hash_key=f"{entity_id}-{type}",
                    filter_condition=Entity.type == type,
                )
            )

    def find_videos_in_channel(
        self, channel_id: str
    ) -> T.List[VideoChannelAssociation]:
        return self.find_many_by_many(
            type=TypeEnum.VIDEO_CHANNEL_ASSOCIATION,
            entity_id=channel_id,
            lookup_by_left=False,
        )

    def find_channels_that_has_video(
        self, video_id: str
    ) -> T.List[VideoChannelAssociation]:
        return self.find_many_by_many(
            type=TypeEnum.VIDEO_CHANNEL_ASSOCIATION,
            entity_id=video_id,
            lookup_by_left=True,
        )

    def find_videos_in_playlist(
        self, playlist_id: str
    ) -> T.List[VideoPlaylistAssociation]:
        return self.find_many_by_many(
            type=TypeEnum.VIDEO_PLAYLIST_ASSOCIATION,
            entity_id=playlist_id,
            lookup_by_left=False,
        )

    def find_playlists_that_has_video(
        self, video_id: str
    ) -> T.List[VideoPlaylistAssociation]:
        return self.find_many_by_many(
            type=TypeEnum.VIDEO_PLAYLIST_ASSOCIATION,
            entity_id=video_id,
            lookup_by_left=True,
        )

    def find_subscribers_for_user(self, user_id: str) -> T.List[UserSubscribeUser]:
        return self.find_many_by_many(
            type=TypeEnum.USER_SUBSCRIBE_USER,
            entity_id=user_id,
            lookup_by_left=False,
        )

    def find_publisher_that_a_user_subscribed(
        self,
        user_id: str,
    ) -> T.List[UserSubscribeUser]:
        return self.find_many_by_many(
            type=TypeEnum.USER_SUBSCRIBE_USER,
            entity_id=user_id,
            lookup_by_left=True,
        )

    def find_subscribers_for_channel(
        self,
        channel_id: str,
    ) -> T.List[UserSubscribeChannel]:
        return self.find_many_by_many(
            type=TypeEnum.USER_SUBSCRIBE_CHANNEL,
            entity_id=channel_id,
            lookup_by_left=False,
        )

    def find_channel_that_a_user_subscribed(
        self,
        user_id: str,
    ) -> T.List[UserSubscribeChannel]:
        return self.find_many_by_many(
            type=TypeEnum.USER_SUBSCRIBE_CHANNEL,
            entity_id=user_id,
            lookup_by_left=True,
        )


op = OP()

u_alice = op.new_user(id="u-1", name="Alice")
u_bob = op.new_user(id="u-2", name="Bob")
u_cathy = op.new_user(id="u-3", name="Cathy")
u_david = op.new_user(id="u-4", name="David")

v_alice_1 = op.new_video(id="v-1-1", name="Alice's Video 1", user_id="u-1")
v_alice_2 = op.new_video(id="v-1-2", name="Alice's Video 2", user_id="u-1")

v_bob_1 = op.new_video(id="v-2-1", name="Bob's Video 1", user_id="u-2")
v_bob_2 = op.new_video(id="v-2-2", name="Bob's Video 2", user_id="u-2")
v_bob_3 = op.new_video(id="v-2-3", name="Bob's Video 3", user_id="u-2")
v_bob_4 = op.new_video(id="v-2-4", name="Bob's Video 4", user_id="u-2")

c_alice_1 = op.new_channel(id="c-1-1", name="Alice's Channel 1", user_id="u-1")

c_bob_1 = op.new_channel(id="c-2-1", name="Bob's Channel 1", user_id="u-2")
c_bob_2 = op.new_channel(id="c-2-2", name="Bob's Channel 2", user_id="u-2")

p_cathy_1 = op.new_playlist(id="p-3-1", name="Cathy's Playlist 1", user_id="u-3")
p_cathy_2 = op.new_playlist(id="p-3-2", name="Cathy's Playlist 2", user_id="u-3")

op.add_video_to_channel(video_id="v-2-1", channel_id="c-2-1")
op.add_video_to_channel(video_id="v-2-2", channel_id="c-2-1")
op.add_video_to_channel(video_id="v-2-3", channel_id="c-2-1")

op.add_video_to_channel(video_id="v-2-2", channel_id="c-2-2")
op.add_video_to_channel(video_id="v-2-3", channel_id="c-2-2")
op.add_video_to_channel(video_id="v-2-4", channel_id="c-2-2")

op.add_video_to_playlist(video_id="v-2-1", playlist_id="p-3-1")
op.add_video_to_playlist(video_id="v-2-2", playlist_id="p-3-1")
op.add_video_to_playlist(video_id="v-2-3", playlist_id="p-3-1")

op.add_video_to_playlist(video_id="v-2-2", playlist_id="p-3-2")
op.add_video_to_playlist(video_id="v-2-3", playlist_id="p-3-2")
op.add_video_to_playlist(video_id="v-2-4", playlist_id="p-3-2")

op.subscribe_user(subscriber_id="u-2", publisher_id="u-1")
op.subscribe_user(subscriber_id="u-3", publisher_id="u-1")
op.subscribe_user(subscriber_id="u-4", publisher_id="u-1")
op.subscribe_user(subscriber_id="u-1", publisher_id="u-2")
op.subscribe_user(subscriber_id="u-3", publisher_id="u-2")
op.subscribe_user(subscriber_id="u-4", publisher_id="u-3")

op.subscribe_channel(subscriber_id="u-1", channel_id="c-2-1")
op.subscribe_channel(subscriber_id="u-1", channel_id="c-2-2")
op.subscribe_channel(subscriber_id="u-2", channel_id="c-1-1")
op.subscribe_channel(subscriber_id="u-3", channel_id="c-1-1")
op.subscribe_channel(subscriber_id="u-3", channel_id="c-2-1")
op.subscribe_channel(subscriber_id="u-4", channel_id="c-2-2")

print("--- Scan entities and relationships ---")
for entity in op.scan():
    entity.print()

print("--- Alice owned videos ---")
for video in op.find_videos_created_by_a_user(user_id="u-1"):
    video.print()

print("--- Bob owned videos ---")
for video in op.find_videos_created_by_a_user(user_id="u-2"):
    video.print()

print("--- Alice owned channels ---")
for channel in op.find_channels_created_by_a_user(user_id="u-1"):
    channel.print()

print("--- Bob owned channels ---")
for channel in op.find_channels_created_by_a_user(user_id="u-2"):
    channel.print()

print("--- Cathy owned playlists ---")
for playlist in op.find_playlists_created_by_a_user(user_id="u-3"):
    playlist.print()

print("--- Videos in Bob's Channel 1 ---")
for video in op.find_videos_in_channel(channel_id="c-2-1"):
    video.print()

print("--- Videos in Bob's Channel 2 ---")
for video in op.find_videos_in_channel(channel_id="c-2-2"):
    video.print()

print("--- Channels that has in Bob's Video 1 ---")
for channel in op.find_channels_that_has_video(video_id="v-2-1"):
    channel.print()

print("--- Channels that has in Bob's Video 2 ---")
for channel in op.find_channels_that_has_video(video_id="v-2-2"):
    channel.print()

print("--- Channels that has in Bob's Video 3 ---")
for channel in op.find_channels_that_has_video(video_id="v-2-3"):
    channel.print()

print("--- Channels that has in Bob's Video 4 ---")
for channel in op.find_channels_that_has_video(video_id="v-2-4"):
    channel.print()

print("--- Videos in Cathy's Playlist 1 ---")
for video in op.find_videos_in_playlist(playlist_id="p-3-1"):
    video.print()

print("--- Videos in Cathy's Playlist 2 ---")
for video in op.find_videos_in_playlist(playlist_id="p-3-2"):
    video.print()

print("--- Playlist that has in Bob's Video 1 ---")
for channel in op.find_playlists_that_has_video(video_id="v-2-1"):
    channel.print()

print("--- Playlist that has in Bob's Video 2 ---")
for channel in op.find_playlists_that_has_video(video_id="v-2-2"):
    channel.print()

print("--- Playlist that has in Bob's Video 3 ---")
for channel in op.find_playlists_that_has_video(video_id="v-2-3"):
    channel.print()

print("--- Playlist that has in Bob's Video 4 ---")
for channel in op.find_playlists_that_has_video(video_id="v-2-4"):
    channel.print()

print("--- Users who subscribes Alice ---")
for user in op.find_subscribers_for_user(user_id="u-1"):
    user.print()

print("--- Users who subscribes Bob ---")
for user in op.find_subscribers_for_user(user_id="u-2"):
    user.print()

print("--- Users who subscribes Cathy ---")
for user in op.find_subscribers_for_user(user_id="u-3"):
    user.print()

print("--- Users who subscribes David ---")
for user in op.find_subscribers_for_user(user_id="u-4"):
    user.print()

print("--- Alice subscribed who ---")
for user in op.find_publisher_that_a_user_subscribed(user_id="u-1"):
    user.print()

print("--- Bob subscribed who ---")
for user in op.find_publisher_that_a_user_subscribed(user_id="u-2"):
    user.print()

print("--- Cathy subscribed who ---")
for user in op.find_publisher_that_a_user_subscribed(user_id="u-3"):
    user.print()

print("--- David subscribed who ---")
for user in op.find_publisher_that_a_user_subscribed(user_id="u-4"):
    user.print()

print("--- Users who subscribes Alice' Channel 1 ---")
for user in op.find_subscribers_for_channel(channel_id="c-1-1"):
    user.print()

print("--- Users who subscribes Bob' Channel 1 ---")
for user in op.find_subscribers_for_channel(channel_id="c-2-1"):
    user.print()

print("--- Users who subscribes Bob' Channel 2 ---")
for user in op.find_subscribers_for_channel(channel_id="c-2-2"):
    user.print()

print("--- Alice subscribed channels ---")
for user in op.find_channel_that_a_user_subscribed(user_id="u-1"):
    user.print()

print("--- Bob subscribed channels ---")
for user in op.find_channel_that_a_user_subscribed(user_id="u-2"):
    user.print()

print("--- Cathy subscribed channels ---")
for user in op.find_channel_that_a_user_subscribed(user_id="u-3"):
    user.print()

print("--- David subscribed channels ---")
for user in op.find_channel_that_a_user_subscribed(user_id="u-4"):
    user.print()
