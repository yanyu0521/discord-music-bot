import os
import asyncio
from dataclasses import dataclass
from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands
from dotenv import load_dotenv
import yt_dlp


import os
from pathlib import Path
from dotenv import load_dotenv
import os

load_dotenv()

TOKEN = os.getenv("DISCORD_TOKEN")



if not TOKEN:
    raise RuntimeError("找不到 DISCORD_TOKEN，請確認 .env 是否設定正確")


intents = discord.Intents.default()
intents.voice_states = True

bot = commands.Bot(command_prefix="!", intents=intents)

FFMPEG_OPTIONS = {
    "before_options": "-reconnect 1 -reconnect_streamed 1 -reconnect_delay_max 5",
    "options": "-vn"
}

YTDLP_OPTIONS = {
    "format": "bestaudio/best",
    "quiet": False,
    "no_warnings": False,
    "default_search": "ytsearch1:",
    "ignoreerrors": False,
    "extract_flat": False,
    "noplaylist": True,
    "socket_timeout": 20,
    "source_address": "0.0.0.0",
}


@dataclass
class Song:
    title: str
    webpage_url: str
    stream_url: str
    requester: str


class GuildMusicState:
    def __init__(self):
        self.queue: asyncio.Queue[Song] = asyncio.Queue()
        self.current: Optional[Song] = None
        self.voice_client: Optional[discord.VoiceClient] = None
        self.player_task: Optional[asyncio.Task] = None
        self.idle_task: Optional[asyncio.Task] = None
        self.lock = asyncio.Lock()


music_states: dict[int, GuildMusicState] = {}


def get_state(guild_id: int) -> GuildMusicState:
    if guild_id not in music_states:
        music_states[guild_id] = GuildMusicState()
    return music_states[guild_id]


async def search_youtube(query: str, requester: str) -> list[Song]:
    def extract():
        with yt_dlp.YoutubeDL(YTDLP_OPTIONS) as ydl:
            return ydl.extract_info(query, download=False)

    info = await asyncio.to_thread(extract)

    if not info:
        return []

    entries = []

    if "entries" in info and info["entries"]:
        entries = [e for e in info["entries"] if e]
    else:
        entries = [info]

    songs = []

    for item in entries:
        if not item:
            continue

        stream_url = item.get("url")
        title = item.get("title", "未知歌曲")
        webpage_url = item.get("webpage_url") or item.get("original_url") or query

        if stream_url:
            songs.append(
                Song(
                    title=title,
                    webpage_url=webpage_url,
                    stream_url=stream_url,
                    requester=requester
                )
            )

    return songs


async def start_idle_timer(guild_id: int):
    state = get_state(guild_id)

    if state.idle_task:
        state.idle_task.cancel()

    async def idle_disconnect():
        try:
            await asyncio.sleep(180)
            vc = state.voice_client
            if vc and vc.is_connected() and not vc.is_playing():
                await vc.disconnect()
                state.voice_client = None
                state.current = None
        except asyncio.CancelledError:
            pass

    state.idle_task = asyncio.create_task(idle_disconnect())


async def cancel_idle_timer(guild_id: int):
    state = get_state(guild_id)
    if state.idle_task:
        state.idle_task.cancel()
        state.idle_task = None


async def music_player_loop(guild_id: int):
    state = get_state(guild_id)

    while True:
        try:
            song = await state.queue.get()
            state.current = song

            vc = state.voice_client
            if not vc or not vc.is_connected():
                state.current = None
                continue

            await cancel_idle_timer(guild_id)

            source = discord.FFmpegPCMAudio(song.stream_url, **FFMPEG_OPTIONS)
            finished = asyncio.Event()

            def after_play(error):
                if error:
                    print(f"FFmpeg 播放錯誤: {error}")
                bot.loop.call_soon_threadsafe(finished.set)

            vc.play(source, after=after_play)
            await finished.wait()

            state.current = None

            if state.queue.empty():
                await start_idle_timer(guild_id)

        except Exception as e:
            print(f"播放器錯誤: {e}")
            await start_idle_timer(guild_id)


async def ensure_voice(interaction: discord.Interaction) -> discord.VoiceClient:
    if not interaction.guild:
        raise app_commands.AppCommandError("這個指令只能在伺服器中使用")

    user = interaction.user

    if not isinstance(user, discord.Member) or not user.voice or not user.voice.channel:
        raise app_commands.AppCommandError("你要先加入一個語音頻道")

    channel = user.voice.channel
    permissions = channel.permissions_for(interaction.guild.me)

    if not permissions.connect:
        raise app_commands.AppCommandError("Bot 沒有加入語音頻道的權限")

    if not permissions.speak:
        raise app_commands.AppCommandError("Bot 沒有在語音頻道說話的權限")

    state = get_state(interaction.guild.id)

    if state.voice_client and state.voice_client.is_connected():
        if state.voice_client.channel != channel:
            await state.voice_client.move_to(channel)
    else:
        state.voice_client = await channel.connect()

    return state.voice_client


@bot.event
async def on_ready():
    await bot.tree.sync()
    print(f"已登入：{bot.user}")
    print("Slash commands 已同步")


@bot.tree.command(name="playmusic", description="播放 YouTube 音樂，可輸入連結或歌名")
@app_commands.describe(query="YouTube 連結或歌曲名稱")
async def playmusic(interaction: discord.Interaction, query: str):
    await interaction.response.defer()

    try:
        if not interaction.guild:
            await interaction.followup.send("這個指令只能在伺服器中使用")
            return

        await ensure_voice(interaction)

        state = get_state(interaction.guild.id)

        async with state.lock:
            songs = await search_youtube(query, interaction.user.display_name)

            if not songs:
                await interaction.followup.send("找不到歌曲，請換個關鍵字或貼 YouTube 連結")
                return

            for song in songs:
                await state.queue.put(song)

            if not state.player_task or state.player_task.done():
                state.player_task = asyncio.create_task(music_player_loop(interaction.guild.id))

            if len(songs) == 1:
                await interaction.followup.send(
                    f"已加入佇列：**{songs[0].title}**\n點歌者：{interaction.user.display_name}"
                )
            else:
                await interaction.followup.send(
                    f"已加入播放清單，共 **{len(songs)}** 首歌\n第一首：**{songs[0].title}**"
                )

    except app_commands.AppCommandError as e:
        await interaction.followup.send(str(e))
    except discord.Forbidden:
        await interaction.followup.send("Bot 權限不足，請檢查 Connect / Speak 權限")
    except Exception as e:
        await interaction.followup.send(f"播放時發生錯誤：{e}")


@bot.tree.command(name="stop", description="暫停目前播放，不清空佇列")
async def stop(interaction: discord.Interaction):
    if not interaction.guild:
        await interaction.response.send_message("這個指令只能在伺服器中使用", ephemeral=True)
        return

    state = get_state(interaction.guild.id)
    vc = state.voice_client

    if not vc or not vc.is_connected():
        await interaction.response.send_message("Bot 目前不在語音頻道")
        return

    if vc.is_playing():
        vc.pause()
        await start_idle_timer(interaction.guild.id)
        await interaction.response.send_message("已暫停播放，3 分鐘內未繼續會自動離開")
    else:
        await start_idle_timer(interaction.guild.id)
        await interaction.response.send_message("目前沒有正在播放的音樂，3 分鐘後會自動離開")


@bot.tree.command(name="start", description="繼續播放暫停的音樂")
async def start(interaction: discord.Interaction):
    if not interaction.guild:
        await interaction.response.send_message("這個指令只能在伺服器中使用", ephemeral=True)
        return

    state = get_state(interaction.guild.id)
    vc = state.voice_client

    if not vc or not vc.is_connected():
        await interaction.response.send_message("Bot 目前不在語音頻道")
        return

    if vc.is_paused():
        vc.resume()
        await cancel_idle_timer(interaction.guild.id)
        await interaction.response.send_message("繼續播放")
    else:
        await interaction.response.send_message("目前沒有暫停中的音樂")


@bot.tree.command(name="skip", description="跳過目前歌曲")
async def skip(interaction: discord.Interaction):
    if not interaction.guild:
        await interaction.response.send_message("這個指令只能在伺服器中使用", ephemeral=True)
        return

    state = get_state(interaction.guild.id)
    vc = state.voice_client

    if not vc or not vc.is_connected():
        await interaction.response.send_message("Bot 目前不在語音頻道")
        return

    if vc.is_playing() or vc.is_paused():
        vc.stop()

        if state.queue.empty():
            await start_idle_timer(interaction.guild.id)
            await interaction.response.send_message("已跳過，目前佇列為空，3 分鐘後會自動離開")
        else:
            await interaction.response.send_message("已跳過，準備播放下一首")
    else:
        await start_idle_timer(interaction.guild.id)
        await interaction.response.send_message("目前沒有正在播放的歌曲")


bot.run(TOKEN)