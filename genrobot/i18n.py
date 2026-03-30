import os
from typing import Optional

_current_lang: Optional[str] = None

TRANSLATIONS = {
    'en': {
        # 命令帮助字符串
        'app_help': 'GenRobot CLI - Embodied Intelligence Multimodal Dataset Download Tool',
        'help_version': 'Show version',
        'help_auth_group': 'Authentication management',
        'help_auth_login': 'Login with username and password',
        'help_auth_login_username': 'Username',
        'help_auth_login_password': 'Password',
        'help_auth_login_endpoint': 'API endpoint URL',
        'help_auth_whoami': 'Show current logged-in user',
        'help_auth_logout': 'Logout',
        'help_dataset_group': 'Dataset management',
        'help_dataset_list': 'List subscribed datasets',
        'help_dataset_stats': 'Show dataset statistics',
        'help_dataset_token': 'Dataset token',
        'help_download': 'Download a dataset',
        'help_download_token': 'Dataset token or name to download',
        'dataset_resolved': 'Resolved "{}" → token: {}',
        'help_download_output_dir': 'Output directory',
        'help_download_concurrency': 'Concurrent downloads (1-16)',
        'help_download_organize_by_sst': 'Organize files by SST (Domain/Scenario/Task/Skill) directory structure',
        'help_download_skip_existing': 'Skip already downloaded files',
        'help_download_verbose': 'Enable verbose log output',
        'help_download_quiet': 'Quiet mode, suppress output',
        # 运行时输出字符串
        'login_success': '✓ Login successful! Token saved',
        'login_failed': '✗ Error: Invalid username or password',
        'network_error': '✗ Error: Cannot connect to server, please check network',
        'server_error': '✗ Error: Server error ({}), please try again later',
        'not_logged_in': '✗ Not logged in, please run: genrobot auth login',
        'token_expired': '✗ Token expired, please run: genrobot auth login',
        'logged_out': '✓ Logged out',
        'current_user': 'Current user: {}',
        'partner_id': 'Partner ID: {}',
        'token_expires': 'Token expires: {} ({} days remaining)',
        'no_datasets': 'No subscribed datasets found',
        'available_datasets': 'Available datasets:',
        'dataset_label': 'Dataset: {}',
        'dataset_name': 'Name: {}',
        'dataset_desc': 'Description: {}',
        'file_count': 'Files: {}',
        'total_size': 'Total size: {}',
        'subscription_status': 'Subscription: {}',
        'subscription_expires': 'Expires: {}',
        'concurrency_range_error': '✗ Error: Concurrency must be between 1 and 16',
        'downloading': 'Downloading',
        'download_complete': 'Download complete!',
        'download_partial_fail': 'Download completed with errors',
        'success_summary': '✓ Success: {} files ({})',
        'skipped_summary': '⊘ Skipped: {} files',
        'failed_summary': '✗ Failed: {} files',
        'total_time': 'Total time: {}',
        'avg_speed': 'Average speed: {:.1f} MB/s',
        'run_id_label': 'Run ID: {}',
        'fetching_metadata': 'Fetching dataset info...',
        'dataset_info': 'Dataset: {}',
        'dataset_file_size': 'Files: {:,} | Total size: {}',
        'download_start': 'Starting download ({} concurrent, streaming batch)...',
        'stats_header': 'Statistics:',
        'files_unit': 'files',
        'eta_label': 'ETA',
        'avg_speed_label': 'avg',
        'cur_speed_label': 'cur',
        'elapsed_seconds': '{}s',
        'elapsed_minutes': '{}m{}s',
        'elapsed_hours': '{}h{}m',
        'found_files': 'Found {} files | Total size: {}',
        'need_download': 'Need to download {} files (skipped {})',
        'all_downloaded': '✓ All files already downloaded (skipped {})',
        'url_expired_reissue': 'URL expired, re-issuing for {} remaining files...',
        'col_name': 'Name',
        'col_token': 'Token',
        'col_duration': 'Duration',
        'col_size': 'Size',
        'col_files': 'Files',
        'col_scenarios': 'Scenarios',
        'col_skills': 'Skills',
        'not_subscribed': '✗ Error: Not subscribed to this dataset',
        'subscription_expired_err': '✗ Error: Subscription expired',
        'interrupt_cleaning': 'Stopping... waiting for in-progress downloads to finish (press Ctrl+C again to force exit)',
        'interrupt_force': 'Force exit',
        'download_cancelled': 'Download cancelled by user',
        'help_dataset_identifier': 'Dataset token or name',
        'stats_section_dataset': 'Dataset Details',
        'stats_section_statistics': 'Statistics',
        'stats_section_subscription': 'Subscription',
        'stats_total_duration': 'Total duration',
        'stats_scenario_count': 'Scenarios',
        'stats_skill_count': 'Skills',
        'stats_purchase_time': 'Purchase time',
        'stats_days_remaining': 'Days remaining',
        'stats_label_description': 'Description',
        'stats_label_total_size': 'Total size',
        'stats_label_expires': 'Expires',
    },
    'zh': {
        # 命令帮助字符串
        'app_help': 'GenRobot CLI - 具身智能多模态数据集下载工具',
        'help_version': '显示版本号',
        'help_auth_group': '认证管理',
        'help_auth_login': '使用用户名和密码登录',
        'help_auth_login_username': '用户名',
        'help_auth_login_password': '密码',
        'help_auth_login_endpoint': 'API 端点 URL',
        'help_auth_whoami': '查看当前登录用户',
        'help_auth_logout': '退出登录',
        'help_dataset_group': '数据集管理',
        'help_dataset_list': '列出已订阅的数据集',
        'help_dataset_stats': '查看数据集统计信息',
        'help_dataset_token': '数据集 token',
        'help_download': '下载数据集',
        'help_download_token': '要下载的数据集 token 或名称',
        'dataset_resolved': '已将 "{}" 解析为 token: {}',
        'help_download_output_dir': '输出目录',
        'help_download_concurrency': '并发下载数 (1-16)',
        'help_download_organize_by_sst': '按 SST（Domain/Scenario/Task/Skill）层级组织目录',
        'help_download_skip_existing': '跳过已下载的文件',
        'help_download_verbose': '输出详细日志',
        'help_download_quiet': '静默模式，抑制输出',
        # 运行时输出字符串
        'login_success': '✓ 登录成功！Token 已保存',
        'login_failed': '✗ 错误: 用户名或密码不正确',
        'network_error': '✗ 错误: 无法连接到服务器，请检查网络',
        'server_error': '✗ 错误: 服务器错误 ({})，请稍后重试',
        'not_logged_in': '✗ 未登录，请先执行: genrobot auth login',
        'token_expired': '✗ Token 已过期，请重新登录: genrobot auth login',
        'logged_out': '✓ 已退出登录',
        'current_user': '当前用户: {}',
        'partner_id': 'Partner ID: {}',
        'token_expires': 'Token 有效期: {} (剩余 {} 天)',
        'no_datasets': '未找到已订阅的数据集',
        'available_datasets': '可用数据集:',
        'dataset_label': '数据集: {}',
        'dataset_name': '名称: {}',
        'dataset_desc': '描述: {}',
        'file_count': '文件数: {}',
        'total_size': '总大小: {}',
        'subscription_status': '订阅状态: {}',
        'subscription_expires': '有效期至: {}',
        'concurrency_range_error': '✗ 错误: 并发数必须在 1-16 之间',
        'downloading': '正在下载',
        'download_complete': '下载完成！',
        'download_partial_fail': '下载完成，部分文件失败',
        'success_summary': '✓ 成功: {} 个文件 ({})',
        'skipped_summary': '⊘ 跳过: {} 个文件',
        'failed_summary': '✗ 失败: {} 个文件',
        'total_time': '总耗时: {}',
        'avg_speed': '平均速度: {:.1f} MB/s',
        'run_id_label': 'Run ID: {}',
        'fetching_metadata': '正在获取数据集信息...',
        'dataset_info': '数据集: {}',
        'dataset_file_size': '文件数: {:,} | 总大小: {}',
        'download_start': '开始下载（{}个并发，流式分批处理）...',
        'stats_header': '统计信息:',
        'files_unit': '文件',
        'eta_label': '预计',
        'avg_speed_label': '平均',
        'cur_speed_label': '当前',
        'elapsed_seconds': '{}秒',
        'elapsed_minutes': '{}分{}秒',
        'elapsed_hours': '{}小时{}分',
        'found_files': '找到 {} 个文件 | 总大小: {}',
        'need_download': '需要下载 {} 个文件 (跳过 {})',
        'all_downloaded': '✓ 所有文件已下载 (跳过 {})',
        'url_expired_reissue': 'URL 已过期，正在为 {} 个剩余文件重新签发...',
        'col_name': '名称',
        'col_token': 'Token',
        'col_duration': '时长',
        'col_size': '大小',
        'col_files': '文件数',
        'col_scenarios': '场景数',
        'col_skills': '技能数',
        'not_subscribed': '✗ 错误: 未订阅该数据集',
        'subscription_expired_err': '✗ 错误: 订阅已过期',
        'interrupt_cleaning': '正在停止... 等待进行中的下载完成（再次按 Ctrl+C 强制退出）',
        'interrupt_force': '强制退出',
        'download_cancelled': '下载已被用户取消',
        'help_dataset_identifier': '数据集 token 或名称',
        'stats_section_dataset': '数据集详情',
        'stats_section_statistics': '统计信息',
        'stats_section_subscription': '订阅信息',
        'stats_total_duration': '总时长',
        'stats_scenario_count': '场景数',
        'stats_skill_count': '技能数',
        'stats_purchase_time': '购买时间',
        'stats_days_remaining': '剩余天数',
        'stats_label_description': '描述',
        'stats_label_total_size': '总大小',
        'stats_label_expires': '有效期至',
    },
}


def detect_language() -> str:
    # 按优先级检测常见语言环境变量，覆盖 Linux/macOS/Windows 场景：
    # LANGUAGE  - GNU gettext 优先使用的多语言列表（如 zh_CN:en）
    # LC_ALL    - 强制覆盖所有 locale 分类
    # LC_MESSAGES - 专门控制消息语言
    # LANG      - 系统默认 locale（最常见）
    # GENROBOT_LANG - 工具专属覆盖变量，优先级最高
    for var in ('GENROBOT_LANG', 'LANGUAGE', 'LC_ALL', 'LC_MESSAGES', 'LANG'):
        val = os.environ.get(var, '').strip()
        if not val or val == 'C' or val == 'POSIX':
            continue
        # LANGUAGE 可能是冒号分隔的列表，取第一个
        first = val.split(':')[0]
        if first.startswith('zh'):
            return 'zh'
        # 只要有明确非中文的 locale，立即返回英文
        if first:
            return 'en'
    return 'en'


def get_language() -> str:
    global _current_lang
    if _current_lang is None:
        _current_lang = detect_language()
    return _current_lang


def t(key: str, *args) -> str:
    """翻译函数"""
    lang = get_language()
    text = TRANSLATIONS.get(lang, TRANSLATIONS['en']).get(key, key)
    if args:
        return text.format(*args)
    return text
