import json
import logging
import os
import re
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from random import randint
from typing import Any, Dict, List, Optional, Set, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- 配置 ---
# 数据库文件名
DB_FILE = 'contacts.db'
# 抓取成员列表时的分页大小
PAGE_LIMIT = 30
# 并发线程数
MAX_WORKERS = 10
# 母组织id
CORP_ID = 1688800000000000

# 状态/日志文件
STATE_FILE = 'dept_state.json'
EMPTY_DEPT_FILE = 'empty_departments.json'
LOG_DIR = 'logs'
RUN_LOG_FILE = os.path.join(LOG_DIR, 'crawler.log')
EMPTY_LOG_FILE = os.path.join(LOG_DIR, 'empty_departments.log')
FAILED_MEMBERS_FILE = os.path.join(LOG_DIR, 'failed_members.log')

# 请求相关
REQUEST_TIMEOUT = 18
MAX_REQUEST_RETRIES = 3
RETRY_BACKOFF = 0.6
MEMBER_MAX_RETRIES = 3
MEMBER_RETRY_BACKOFF = 0.8

# --- URL ---
# 1. 获取组织架构 URL
CONTACTS_URL = 'https://work.weixin.qq.com/wework_admin/pc_contacts'
# 2. 获取组织成员 URL
GET_PARTY_MEMBER_URL = 'https://work.weixin.qq.com/wework_admin/pc_contacts/get_party_member'
# 3. 获取成员详情 URL
GET_MEMBER_URL = 'https://work.weixin.qq.com/wework_admin/h5_contacts/member'

# --- 线程锁 ---
db_lock = threading.Lock()
state_lock = threading.RLock()


def ensure_dir(path: str):
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)


def chunked(iterable: List[Any], size: int) -> List[List[Any]]:
    return [iterable[i:i + size] for i in range(0, len(iterable), size)]


def load_json_file(path: str, default: Any):
    if not os.path.exists(path):
        return default
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return default


def save_json_file(path: str, data: Any):
    ensure_dir(os.path.dirname(path) or '.')
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def setup_logger() -> logging.Logger:
    ensure_dir(LOG_DIR)
    logger = logging.getLogger('wechat_scraper')
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()

    formatter = logging.Formatter('[%(asctime)s] %(levelname)s - %(message)s', '%Y-%m-%d %H:%M:%S')

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    file_handler = logging.FileHandler(RUN_LOG_FILE, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


class DepartmentState:
    def __init__(self, state_file: str):
        self.state_file = state_file
        self.data = load_json_file(self.state_file, {})

    def _persist(self):
        with state_lock:
            save_json_file(self.state_file, self.data)

    def mark(self, party_id: str, status: str, name: str = '', meta: Optional[Dict[str, Any]] = None):
        with state_lock:
            entry = self.data.get(party_id, {})
            entry.update({
                'status': status,
                'name': name or entry.get('name', ''),
                'updated_at': time.time(),
                'meta': meta or entry.get('meta', {})
            })
            self.data[party_id] = entry
            self._persist()

    def bump_failure(self, party_id: str, name: str, reason: str):
        with state_lock:
            entry = self.data.get(party_id, {})
            fail_count = entry.get('fail_count', 0) + 1
            entry.update({
                'status': 'failed',
                'name': name or entry.get('name', ''),
                'fail_count': fail_count,
                'last_error': reason,
                'updated_at': time.time()
            })
            self.data[party_id] = entry
            self._persist()

    def get_status(self, party_id: str) -> Optional[str]:
        return self.data.get(party_id, {}).get('status')

    def filter_targets(self, party_ids: List[str]) -> List[str]:
        targets = []
        for pid in party_ids:
            status = self.get_status(pid)
            if status in (None, 'pending', 'failed'):
                targets.append(pid)
        return targets


class EmptyDepartmentCache:
    def __init__(self, cache_file: str):
        self.cache_file = cache_file
        self.data = load_json_file(cache_file, {})
        self.lock = threading.Lock()

    def contains(self, party_id: str) -> bool:
        return party_id in self.data

    def add(self, party_id: str, name: str):
        with self.lock:
            if party_id not in self.data:
                self.data[party_id] = {'name': name, 'recorded_at': time.time()}
                save_json_file(self.cache_file, self.data)


class Database:
    """
    数据库操作类，用于处理SQLite数据库
    """
    def __init__(self, db_file: str):
        self.db_file = db_file
        self.init_db()

    def init_db(self):
        """
        初始化数据库，如果表不存在则创建
        """
        # 使用锁确保表创建的原子性
        with db_lock:
            try:
                with sqlite3.connect(self.db_file) as conn:
                    cursor = conn.cursor()
                    cursor.execute('''
                    CREATE TABLE IF NOT EXISTS contacts (
                        uin TEXT PRIMARY KEY,
                        username TEXT,
                        realname TEXT,
                        mobile TEXT,
                        email TEXT,
                        path TEXT
                    )
                    ''')
                    conn.commit()
            except sqlite3.Error as e:
                print(f"数据库初始化失败: {e}")

    def save_contacts(self, contacts: List[tuple]):
        """
        批量保存联系人数据到数据库
        使用 INSERT OR IGNORE 语句，让数据库自动处理重复记录
        """
        if not contacts:
            return
        with db_lock:
            try:
                with sqlite3.connect(self.db_file) as conn:
                    cursor = conn.cursor()
                    cursor.executemany('''
                    INSERT OR IGNORE INTO contacts (uin, username, realname, mobile, email, path)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ''', contacts)
                    conn.commit()
            except sqlite3.Error as e:
                print(f"数据库批量保存失败: {e}")

    def get_existing_uins(self, uins: List[str]) -> Set[str]:
        """
        批量查询哪些 uin 已存在于数据库中，返回存在的 uin 集合
        """
        if not uins:
            return set()

        existing: Set[str] = set()
        with db_lock:
            try:
                with sqlite3.connect(self.db_file) as conn:
                    cursor = conn.cursor()
                    for chunk in chunked(uins, 500):
                        placeholders = ','.join('?' for _ in chunk)
                        try:
                            cursor.execute(f"SELECT uin FROM contacts WHERE uin IN ({placeholders})", chunk)
                            existing.update({row[0] for row in cursor.fetchall()})
                        except sqlite3.Error:
                            continue
            except sqlite3.Error as e:
                print(f"查询已存在 uin 失败: {e}")
        return existing

class WeChatScraper:
    """企业微信爬虫"""

    def __init__(self, sid: str, db: Database, state: DepartmentState, empty_cache: EmptyDepartmentCache, logger: logging.Logger):
        self.db = db
        self.state = state
        self.empty_cache = empty_cache
        self.logger = logger
        self.session = requests.Session()
        self.session.cookies.set('h5_contacts_sid', sid)
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36 NetType/WIFI MicroMessenger/7.0.20.1781(0x6700143B) WindowsWechat(0x63090a13) UnifiedPCWindowsWechat(0xf2541411) XWEB/16965',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Priority': 'u=0, i'
        })
        self._configure_retries()

    def _configure_retries(self):
        retry = Retry(
            total=MAX_REQUEST_RETRIES,
            read=MAX_REQUEST_RETRIES,
            connect=MAX_REQUEST_RETRIES,
            backoff_factor=RETRY_BACKOFF,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )
        adapter = HTTPAdapter(max_retries=retry, pool_maxsize=MAX_WORKERS)
        self.session.mount('https://', adapter)
        self.session.mount('http://', adapter)

    def _parse_departments_from_text(self, text: str) -> Dict[str, Any]:
        match = re.search(r"window\.dep\s*=\s*(['\"])(.*?)(\1)\s*\|\|", text)
        if not match:
            return {}
        dep_json = match.group(2)
        try:
            data = json.loads(dep_json)
            return data.get('dep', {})
        except (json.JSONDecodeError, IndexError):
            return {}

    def load_departments(self) -> Dict[str, Any]:
        params = {
            'from': 'chat',
            'type': 'contact',
            'chatid': f"ww{CORP_ID}@qy_u",
            'src': 'conversation',
            'state': '123'
        }
        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/wxpic,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Dest': 'document',
            'Referer': 'https://work.weixin.qq.com/wework_admin/h5_contacts',
        }
        try:
            response = self.session.get(CONTACTS_URL, params=params, headers=headers, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            deps = self._parse_departments_from_text(response.text)
            if not deps:
                self.logger.error('HTML 中未找到 window.dep，无法继续。API返回: %s', response.text)
                return {}
            return deps
        except requests.RequestException as exc:
            if 'response' in locals():
                self.logger.error('获取组织架构失败: %s，API返回: %s', exc, getattr(response, 'text', '无返回'))
            else:
                self.logger.error('获取组织架构失败: %s', exc)
            return {}

    def get_all_department_ids(self, all_deps: Dict[str, Any]) -> List[str]:
        """
        获取所有组织ID（直接从键中提取）
        父组织和子组织都可能包含成员，因此需要遍历所有组织ID
        """
        return list(all_deps.keys())

    def get_department_members(self, party_id: str) -> List[Dict[str, Any]]:
        """
        步骤2: 获取单个组织下的所有成员（处理分页）
        """
        all_members = []
        offset = 0
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'X-Requested-With': 'XMLHttpRequest',
            'Origin': 'https://work.weixin.qq.com',
            'Referer': 'https://work.weixin.qq.com/wework_admin/pc_contacts',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Dest': 'empty',
        }
        while True:
            data = {
                'partyid': party_id,
                'offset': offset,
                'limit': PAGE_LIMIT
            }
            try:
                time.sleep(0.1544 * randint(3, 15))  # 礼貌性延迟
                response = self.session.post(GET_PARTY_MEMBER_URL, data=data, headers=headers, timeout=REQUEST_TIMEOUT)
                response.raise_for_status()
                json_data = response.json()
                members = json_data.get('data', {}).get('contact_user', [])
                if not members:
                    break # 没有更多成员
                all_members.extend(members)
                if not json_data.get('data', {}).get('has_next_page', False):
                    break
                offset += PAGE_LIMIT
            except (requests.RequestException, json.JSONDecodeError) as exc:
                err_text = ''
                if 'response' in locals():
                    try:
                        err_text = getattr(response, 'text', '')
                    except Exception:
                        err_text = ''
                raise RuntimeError(f'组织 {party_id} 成员获取失败: {exc}，API返回: {err_text}')
        return all_members

    def get_member_details(self, user_id: str) -> Dict[str, Any]:
        """
        步骤3: 获取单个成员的详细信息
        """
        uin = f"ww{user_id}@qy_u"
        params = {
            'uin': uin,
            'state': 'qyhchatlogic_oauth'
        }
        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/wxpic,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Dest': 'document',
        }
        try:
            time.sleep(0.1544 * randint(3, 15)) # 礼貌性延迟
            response = self.session.get(GET_MEMBER_URL, params=params, headers=headers, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            match = re.search(r"window\.contact = '({.*})'", response.text)
            if match:
                contact_json = match.group(1)
                return json.loads(contact_json)
            else:
                self.logger.error('在HTML中未找到 %s 的 window.contact，API返回: %s', uin, response.text)
                raise RuntimeError(f'在HTML中未找到 {uin} 的 window.contact')
        except (requests.RequestException, json.JSONDecodeError) as exc:
            err_text = ''
            if 'response' in locals():
                try:
                    err_text = getattr(response, 'text', '')
                except Exception:
                    err_text = ''
            self.logger.error('获取成员 %s 详情失败: %s，API返回: %s', uin, exc, err_text)
            raise RuntimeError(f'获取成员 {uin} 详情失败: {exc}，API返回: {err_text}')

    def _fetch_member_details_with_retry(self, user_id: str) -> Optional[Dict[str, Any]]:
        delay = MEMBER_RETRY_BACKOFF
        for attempt in range(1, MEMBER_MAX_RETRIES + 1):
            try:
                return self.get_member_details(user_id)
            except RuntimeError as exc:
                self.logger.warning(
                    '成员 %s 详情获取失败，第 %d/%d 次尝试: %s',
                    user_id,
                    attempt,
                    MEMBER_MAX_RETRIES,
                    exc
                )
                if attempt == MEMBER_MAX_RETRIES:
                    break
                time.sleep(delay + 0.13 * randint(1, 3))
                delay *= 1.5
        return None

    def _record_member_failures(self, party_id: str, party_name: str, member_ids: List[str]):
        if not member_ids:
            return
        ensure_dir(LOG_DIR)
        line = json.dumps({
            'party_id': party_id,
            'party_name': party_name,
            'failed_member_ids': member_ids,
            'timestamp': time.time()
        }, ensure_ascii=False)
        with open(FAILED_MEMBERS_FILE, 'a', encoding='utf-8') as f:
            f.write(line + '\n')

    def process_department(self, party_id: str, info: List[Any]):
        name = info[0] if info else ''

        if self.empty_cache.contains(party_id):
            self.logger.debug('组织 %s(%s) 已被判定为空，跳过。', name, party_id)
            return

        status = self.state.get_status(party_id)
        if status == 'done':
            self.logger.debug('组织 %s(%s) 已完成，跳过。', name, party_id)
            return

        self.state.mark(party_id, 'running', name)
        self.logger.info('开始处理组织 %s (%s)', name or '未知', party_id)
        time.sleep(0.2 * randint(1, 4))

        try:
            members = self.get_department_members(party_id)
        except RuntimeError as exc:
            self.logger.warning('组织 %s(%s) 获取成员失败: %s', name, party_id, exc)
            self.state.bump_failure(party_id, name, str(exc))
            return

        if not members:
            self.logger.info('组织 %s(%s) 无成员，记为空组织。', name, party_id)
            self._record_empty_department(party_id, name)
            self.state.mark(party_id, 'empty', name)
            return

        member_id_to_uin: Dict[str, str] = {}
        uins_for_query: List[str] = []
        for m in members:
            mid = m.get('id')
            if not mid:
                continue
            constructed = f"ww{mid}@qy_u"
            member_id_to_uin[mid] = constructed
            # 仅加入纯数字 id 用于去重查询
            uins_for_query.append(str(mid))

        existing_uins = self.db.get_existing_uins(uins_for_query) if uins_for_query else set()

        contacts_to_save: List[Tuple[str, str, str, str, str, str]] = []
        failed_member_ids: List[str] = []
        for member in members:
            member_id = member.get('id')
            if not member_id:
                continue

            # 已存在的判断：数据库中存的是纯数字 id，因此只需判断 member_id 是否存在
            if str(member_id) in existing_uins:
                continue

            details = self._fetch_member_details_with_retry(member_id)
            if details is None:
                failed_member_ids.append(member_id)
                continue

            contact = details.get('contact', {})
            path_info = details.get('party_path', {}).get('list', [{}])[0]
            uin = contact.get('uin')
            if not uin:
                self.logger.debug('成员 %s 缺少 uin，跳过。', member_id)
                continue

            path_list = path_info.get('path', []) if isinstance(path_info, dict) else []
            contact_data = (
                uin,
                contact.get('username', ''),
                contact.get('realname', ''),
                contact.get('mobile', ''),
                contact.get('email', ''),
                ' / '.join(path_list)
            )
            contacts_to_save.append(contact_data)
            time.sleep(0.2 * randint(1, 4))

        if contacts_to_save:
            self.db.save_contacts(contacts_to_save)

        if failed_member_ids:
            self._record_member_failures(party_id, name, failed_member_ids)
            self.logger.warning('组织 %s(%s) 有 %d 个成员连续失败，将记录以便后续补采。', name or '未知', party_id, len(failed_member_ids))

        self.state.mark(
            party_id,
            'done',
            name,
            {
                'saved': len(contacts_to_save),
                'failed_members': failed_member_ids
            }
        )
        self.logger.info(
            '完成组织 %s (%s)，新增 %d 条，失败 %d 条。',
            name or '未知',
            party_id,
            len(contacts_to_save),
            len(failed_member_ids)
        )

    def _record_empty_department(self, party_id: str, name: str):
        ensure_dir(LOG_DIR)
        self.empty_cache.add(party_id, name)
        line = json.dumps({
            'party_id': party_id,
            'name': name,
            'timestamp': time.time()
        }, ensure_ascii=False)
        with open(EMPTY_LOG_FILE, 'a', encoding='utf-8') as f:
            f.write(line + '\n')


def main():
    logger = setup_logger()
    sid = input("请输入 h5_contacts_sid: ").strip()
    if not sid:
        logger.error('h5_contacts_sid 不能为空。')
        return

    db = Database(DB_FILE)
    state = DepartmentState(STATE_FILE)
    empty_cache = EmptyDepartmentCache(EMPTY_DEPT_FILE)
    scraper = WeChatScraper(sid, db, state, empty_cache, logger)

    # 将上次运行遗留的 running 状态重置为 pending，避免部门永远不再被处理
    if isinstance(state.data, dict):
        for pid, info in state.data.items():
            if info.get('status') == 'running':
                state.mark(pid, 'pending', info.get('name', ''))
    
    logger.info('步骤 1: 获取组织架构...')
    all_deps = scraper.load_departments()
    if not all_deps:
        logger.error('无法获取组织架构，请检查 SID 或网络。')
        return
    logger.info('成功获取 %d 个组织节点。', len(all_deps))

    all_ids = scraper.get_all_department_ids(all_deps)
    pending_ids = state.filter_targets(all_ids)
    pending_ids = [pid for pid in pending_ids if not empty_cache.contains(pid)]

    for pid in pending_ids:
        info = all_deps.get(pid, [])
        name = info[0] if info else ''
        state.mark(pid, 'pending', name)

    if not pending_ids:
        logger.info('没有需要处理的组织，任务结束。')
        return

    logger.info('本次计划处理 %d 个组织，启用 %d 个线程。', len(pending_ids), MAX_WORKERS)

    department_items = [(pid, all_deps.get(pid, [])) for pid in pending_ids]

    def worker(item: Tuple[str, List[Any]]):
        party_id, info = item
        scraper.process_department(party_id, info or [])

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        list(executor.map(worker, department_items))

    logger.info('所有任务已完成。')

if __name__ == "__main__":
    main()