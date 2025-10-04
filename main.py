import asyncio
import os
import json
from typing import List, Dict, Any
from playwright.async_api import async_playwright

COOKIE = 'c_user=100028314736062; xs=11%3AUKNtV4sJKV2PjQ%3A2%3A1740376157%3A-1%3A-1%3A%3AAcWPQDQL6KNaIxARUyDTHyTRJ_YhgmrfLvVM_wTVXt1v1w; fr=1RczojDhQbCIt4Cx8.AWc6P6mmn1eepXDJIy-mmtoeGgf9i4oIKf251VWgPfM6LpAx408.Bo4NIM..AAA.0.0.Bo4NIM.AWeKhagPUZ0TmgSFyqc2glBrxis; datr=igG8Z8Soz4vFnp6jMwj9R-i7'
TARGET_URL = 'https://www.facebook.com/nasa/'

DATA_DIR = 'data'
RAW_LOG = os.path.join(DATA_DIR, 'graphql_logs.json')
PARSED_OUT = os.path.join(DATA_DIR, 'posts_parsed.json')

os.makedirs(DATA_DIR, exist_ok=True)

#--------------------------------
# Scraping part
#--------------------------------
async def scrape_and_save():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        # Inject cookie
        for c in COOKIE.split('; '):
            name, value = c.split('=', 1)
            await context.add_cookies([{
                'name': name,
                'value': value,
                'domain': '.facebook.com',
                'path': '/'
            }])
        page = await context.new_page()

        # Listening GraphQL responses
        async def on_response(response):
            url = response.url
            if 'graphql' in url:
                try:
                    body = await response.text()
                    # Strip out anti-JSON prefix
                    if body.startswith('for (;;);'):
                        body = body[9:] # get rid of the protection
                    if any(k in body for k in ['comment', 'story', 'feedback','timeline_list_feed_units']):
                        with open(RAW_LOG, 'a', encoding='utf-8') as f:
                            f.write(body + '\n\n')
                        print(f'Saved GraphQL: {url}')
                except Exception as e:
                    print('Error reading response: ', e)
        page.on('response', on_response)

        print('Opening target page...')
        await page.goto(TARGET_URL)
        await page.wait_for_timeout(30000)
        print('Done scraping. Data saved to data/graphql_logs.json')

        await browser.close()

#--------------------------------
# Parsing part
#--------------------------------
def safe_json_loads(s: str) -> Any:
    # Try to decode a JSON string. If there is prefix garbage, try to find first '{' and decode from there.
    try:
        return json.loads(s)
    except json.JSONDecodeError:
        # try to find first '{' or '['
        for ch in ('{', '['):
            idx = s.find(ch)
            if idx != -1:
                try:
                    return json.loads(s[idx:])
                except Exception:
                    pass
    raise

def extract_posts_from_node(node: Dict[str, Any]) -> Dict[str, Any]:
    # Extract useful fields from a feed unit code.
    post = {}
    post['id'] = node.get('id')
    post['type'] = node.get('__typename', 'Story') # usually: Story

    # Text message: look in message or momet message representation
    text = None
    if node.get('message'):
        # sometimes message is nested object
        msg = node.get('message')
        if isinstance(msg, dict):
            # try to get 'message' -> 'text' or ranges
            text = msg.get('message') or msg.get('text')
        else:
            text = msg

    # fallback: some payloads have comet sections with message -> ranges
    if not text:
        # try common path used in examples: node['comet_sections']['content']['story']['message']['ranges']
        try:
            ranges = node.get('comet_sections', {}).get('content', {}).get('story', {}).get('message', {}).get('ranges', [])
            if ranges and isinstance(ranges, list):
                # join text parts
                text = ''.join(r.get('text', '') for r in ranges)
        except Exception:
            text = ''
    post['text'] = text or ''

    # owner
    try:
        owner = node.get('feedback', {}).get('owning_profile', {}).get('name')
    except Exception:
        owner = None
    post['owner'] = owner

    # publish time if present
    publish_time = node.get('publish_time') or node.get('created_time') \
                   or node.get('metadata', {}).get('story', {}).get('creation_time')
    post['publish_time'] = publish_time

    # counts: comments, shares, views
    #comment count common path:
    comment_count = None
    try:
        # several possible paths
        comment_count = node.get('comment_rendering_instance', {}).get('comments', {}).get('total_count')
    except Exception:
        comment_count = None
    if comment_count is None:
        try:
            comment_count = node.get('comments_count_summary_renderer', {}).get('feedback', {}).get('comment_rendering_instance', {}).get('comments', {}).get('total_count')
        except Exception:
            pass

    if comment_count is None:
        # fallback path shown earlier
        try:
            comment_count = node.get('comet_sections', {}).get('content', {}).get('story', {}).get('feedback', {}).get('comments_count_summary_renderer', {}).get('feedback', {}).get('comment_redering_instance', {}).get('comments', {}).get('total_count')
        except Exception:
            pass
    post['comment_count'] = int(comment_count) if isinstance(comment_count, (int, float, str)) and str(comment_count).isdigit() else (comment_count or 0)

    # shares
    shares = node.get('share_count', {}).get('count') or node.get('share_count') or 0
    try:
        post['share_count'] = int(shares)
    except Exception:
        post['share_count'] = shares or 0

    # view count (for videos)
    post['view_count'] = node.get('video_view_count') or node.get('view_count') or 0

    # reactions: try to pull top reactions if present
    reactions = {}
    try:
        # path used in sample: node['comet_sections']... or node['feedback'] variants
        # also check ufi summary:
        ufi = node.get('feedback', {}).get('comet_ufi_summary_and_actions_renderer', {}).get('feedback') \
              or node.get('feedback')
        if ufi:
            # try top_reactions edges
            top = ufi.get('top_reactions', {}).get('edges') if isinstance(ufi, dict) else None
            if top:
                for edge in top:
                    node_r = edge.get('node') if isinstance(edge, dict) else None
                    if node_r:
                        name = node_r.get('localized_name') or node_r.get('id')
                        count = edge.get('reaction_count') or edge.get('i18n_reaction_count')
                        try:
                            reactions[name] = int(count)
                        except Exception:
                            reactions[name] = count
    except Exception:
        pass

    # fallback: try ufi_action_renderers supported_reaction_infos
    if not reactions:
        try:
            for r in node.get('ufi_action_renderers', []):
                fb = r.get('feedback', {})
                for info in fb.get('supported_reaction_infos', []):
                    name = info.get('node', {}).get('localized_name') or info.get('id')
                    cnt = info.get('reaction_count') or info.get('i18n_reaction_count') or 0
                    reactions[name] = int(cnt) if isinstance(cnt, (int,str)) and str(cnt).isdigit() else cnt
        except Exception:
            pass

    post['reactions'] = reactions

    # attachments: collect videos/images/links
    attachments = []
    for att in node.get('attachments', []) or []:
        try:
            media = att.get('media') or att.get('styles', {}).get('attachment', {}).get('media')
            att_obj = {
                'type': media.get('__typename') if isinstance(media, dict) else None,
                'id': media.get('id') if isinstance(media, dict) else None,
                'url': None
            }
            # various places to find url
            url = att.get('href') or att.get('url') or att.get('styles', {}).get('attachment', {}).get('url')
            if not url:
                url = media.get('url') if isinstance(media, dict) else None

            # thumbnail
            thumb = None
            try:
                thumb = media.get('thumbnailImage', {}).get('uri')
            except Exception:
                thumb = None
            if url:
                att_obj['url'] = url
            if thumb:
                att_obj['thumbnail'] = thumb
            attachments.append(att_obj)
        except Exception:
            continue

    post['attachments'] = attachments
    return post

def parse_raw_log_to_posts(raw_path: str) -> List[Dict[str, Any]]:
    # Read RAW_LOG, split the saved responses, parse JSON and extract posts.
    posts = []
    if not os.path.exists(raw_path):
        print('No raw log found at', raw_path)
        return posts
    with open(raw_path, 'r', encoding='utf-8') as f:
        raw = f.read()

    # split entries by two newlines (we appended '\n\n' after each response)
    chunks = [c.strip() for c in raw.split('\n\n') if c.strip()]
    for chunk in chunks:
        try:
            data = safe_json_loads(chunk)
        except Exception as e:
            print('Skipping invalid JSON chunk:', e)
            continue

        # Many responses use path: data.node.timeline_list_feed_units.edges
        try:
            edges = data.get('data', {}).get('node', {}).get('timeline_list_feed_units', {}).get('edges', [])
            if not edges:
                # try otehr ccommon path where top-levell is 'o0' or similar
                # try to find first object that contains 'timeline_list_feed_units
                def find_tlf(obj):
                    if isinstance(obj, dict):
                        for k, v in obj.items():
                            if k == 'timeline_list_feed_units':
                                return obj[k].get('edges', [])
                            else:
                                found = find_tlf(v)
                                if found:
                                    return found
                    return []
                edges = find_tlf(data)
        except Exception:
            edges = []

        for edge in edges:
            node = edge.get('node') or {}
            try:
                post = extract_posts_from_node(node)
                posts.append(post)
            except Exception as e:
                print('Error extracting node:', e)
                continue

    return posts

#--------------------------------
# Main runner: scrape then parse
#--------------------------------
async def main():
    # 1) scrape and append raw GraphQL responses
    await scrape_and_save()

    # 2) parse saved raw logs
    posts = parse_raw_log_to_posts(RAW_LOG)
    # save parsed posts
    with open(PARSED_OUT, 'w', encoding='utf-8') as f:
        json.dump(posts, f, ensure_ascii=False, indent=2)
    print(f'Saved {len(posts)} parsed posts to {PARSED_OUT}')

if __name__ == '__main__':
    asyncio.run(main())