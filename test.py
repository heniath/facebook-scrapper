import os
import json
from typing import List, Dict, Any

DATA_DIR = 'data'
RAW_LOG = os.path.join(DATA_DIR, 'graphql_logs.json')
PARSED_OUT = os.path.join(DATA_DIR, 'posts_parsed.json')

def safe_json_loads(s: str) -> Any:
    """Decode JSON, bỏ prefix bảo vệ kiểu 'for (;;);'"""
    try:
        return json.loads(s)
    except json.JSONDecodeError:
        for ch in ('{', '['):
            idx = s.find(ch)
            if idx != -1:
                try:
                    return json.loads(s[idx:])
                except Exception:
                    pass
    raise

def extract_post_from_node(node: Dict[str, Any]) -> Dict[str, Any]:
    post = {}
    post['id'] = node.get('id')
    post['type'] = node.get('__typename', 'Story')

    # text
    text = None
    if node.get('message'):
        msg = node.get('message')
        if isinstance(msg, dict):
            text = msg.get('message') or msg.get('text')
        else:
            text = msg
    if not text:
        try:
            ranges = node.get('comet_sections', {}).get('content', {}).get('story', {}).get('message', {}).get('ranges', [])
            if ranges:
                text = ''.join(r.get('text', '') for r in ranges)
        except Exception:
            text = ''
    post['text'] = text or ''

    # owner
    owner = node.get('feedback', {}).get('owning_profile', {}).get('name')
    post['owner'] = owner or ''

    # publish_time
    publish_time = node.get('publish_time') or node.get('created_time') \
                   or node.get('metadata', {}).get('story', {}).get('creation_time')
    post['publish_time'] = publish_time

    # comment_count
    comment_count = 0
    try:
        comment_count = node.get('feedback', {}).get('comment_count') \
                        or node.get('comments_count_summary_renderer', {}).get('feedback', {}).get('comment_rendering_instance', {}).get('comments', {}).get('total_count') \
                        or 0
        comment_count = int(comment_count)
    except Exception:
        comment_count = 0
    post['comment_count'] = comment_count

    # share_count
    shares = 0
    try:
        shares = node.get('share_count', {}).get('count') or node.get('share_count') or 0
        shares = int(shares)
    except Exception:
        shares = 0
    post['share_count'] = shares

    # view_count
    view_count = node.get('video_view_count') or node.get('view_count') or 0
    post['view_count'] = view_count

    # reactions
    reactions = {}
    try:
        ufi = node.get('feedback', {}).get('comet_ufi_summary_and_actions_renderer', {}).get('feedback') or node.get('feedback', {})
        if ufi:
            edges = ufi.get('top_reactions', {}).get('edges', [])
            for edge in edges:
                n = edge.get('node', {})
                name = n.get('localized_name') or n.get('id')
                count = edge.get('reaction_count') or edge.get('i18n_reaction_count') or 0
                reactions[name] = int(count) if str(count).isdigit() else count
        # fallback supported_reaction_infos
        if not reactions:
            for r in node.get('ufi_action_renderers', []):
                fb = r.get('feedback', {})
                for info in fb.get('supported_reaction_infos', []):
                    n = info.get('node', {})
                    name = n.get('localized_name') or n.get('id')
                    cnt = info.get('reaction_count') or info.get('i18n_reaction_count') or 0
                    reactions[name] = int(cnt) if str(cnt).isdigit() else cnt
    except Exception:
        reactions = {}
    post['reactions'] = reactions

    # attachments
    attachments = []
    for att in node.get('attachments', []) or []:
        try:
            media = att.get('media') or att.get('styles', {}).get('attachment', {}).get('media')
            att_obj = {
                'type': media.get('__typename') if isinstance(media, dict) else None,
                'id': media.get('id') if isinstance(media, dict) else None,
                'url': None
            }
            url = att.get('href') or att.get('url') or att.get('styles', {}).get('attachment', {}).get('url')
            if not url and media:
                url = media.get('url')
            if not url and att_obj['type'] == 'Photo' and att_obj['id']:
                url = f"https://www.facebook.com/photo.php?fbid={att_obj['id']}"
            att_obj['url'] = url
            attachments.append(att_obj)
        except Exception:
            continue
    post['attachments'] = attachments
    return post

def parse_raw_log_to_posts(raw_path: str) -> List[Dict[str, Any]]:
    posts = []
    if not os.path.exists(raw_path):
        print('No raw log found at', raw_path)
        return posts
    with open(raw_path, 'r', encoding='utf-8') as f:
        raw = f.read()
    chunks = [c.strip() for c in raw.split('\n\n') if c.strip()]
    for chunk in chunks:
        try:
            data = safe_json_loads(chunk)
        except Exception as e:
            print('Skipping invalid JSON chunk:', e)
            continue
        # tìm edges
        edges = []
        try:
            edges = data.get('data', {}).get('node', {}).get('timeline_list_feed_units', {}).get('edges', [])
            if not edges:
                def find_tlf(obj):
                    if isinstance(obj, dict):
                        for k, v in obj.items():
                            if k == 'timeline_list_feed_units':
                                return v.get('edges', [])
                            else:
                                found = find_tlf(v)
                                if found:
                                    return found
                    return []
                edges = find_tlf(data)
        except Exception:
            edges = []
        for edge in edges:
            node = edge.get('node', {})
            try:
                post = extract_post_from_node(node)
                posts.append(post)
            except Exception as e:
                print('Error extracting node:', e)
                continue
    return posts

if __name__ == '__main__':
    posts = parse_raw_log_to_posts(RAW_LOG)
    with open(PARSED_OUT, 'w', encoding='utf-8') as f:
        json.dump(posts, f, ensure_ascii=False, indent=2)
    print(f"Saved {len(posts)} parsed posts to {PARSED_OUT}")
