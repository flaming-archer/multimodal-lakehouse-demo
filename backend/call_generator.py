"""
Random call transcript generator for demo / stress testing.

Generates realistic telecom customer service dialogues with:
- Randomized scenarios (转网/投诉/降套餐/销户/业务咨询/合约等)
- Randomized locations, amounts, competitors
- Variable customer sentiment (angry/calm/confused)
- Unique call_id per generation
"""

import random
import time
from datetime import datetime

# ── Scenario pool ──

SCENARIOS = [
    "转网流失",
    "投诉扣费",
    "投诉服务",
    "投诉网速",
    "降套餐",
    "销户",
    "业务咨询",
    "合约续约",
    "流量咨询",
    "宽带升级",
    "老人套餐",
    "国际漫游",
]

# ── Template parts ──

_GREETINGS = [
    "您好，客服中心，请问有什么可以帮您？",
    "您好，很高兴为您服务，请问有什么可以帮您？",
    "您好，请问您需要什么帮助？",
    "您好，欢迎致电客服中心，我是0886号客服，请问有什么可以帮您？",
]

_LOCATIONS = [
    "北京朝阳区", "上海浦东", "上海静安", "广州天河", "广州越秀",
    "深圳南山", "深圳福田", "杭州西湖", "杭州滨江", "成都高新区",
    "武汉光谷", "南京鼓楼", "重庆渝北", "西安雁塔", "长沙岳麓",
    "天津河西", "苏州工业园区", "合肥蜀山", "厦门思明", "青岛崂山",
]

_COMPETITORS = [
    "联通", "电信", "广电",
]

_PHONE_PREFIXES = ["138", "139", "136", "137", "151", "152", "158", "159", "182", "183"]

_REASONS_PRICE = [
    "你们的套餐太贵了，一个月{amount}，{competitor}那边才{comp_amount}",
    "资费太高了，完全用不起，我朋友{competitor}的套餐比我便宜一半",
    "你们这个价格根本不值，{competitor}同样的套餐只要{comp_amount}",
    "每个月{amount}块钱太贵了，我普通打打电话用不了这么多",
    "性价比太差了，{competitor}那边{comp_amount}的套餐还送流量",
]

_REASONS_SIGNAL = [
    "信号太差了，我在{location}那边经常打不通电话",
    "{location}的5G信号根本就是摆设，看个视频都卡",
    "网速慢得要命，我在{location}测速才{mb}M",
    "在{location}写字楼里完全没信号，客户电话都接不到",
    "家里的信号太差了，在地下室完全打不通",
]

_REASONS_SERVICE = [
    "客服电话太难打了，等了快{wait}分钟",
    "你们客服态度太差了，根本不把客户当回事",
    "我已经投诉过了，但是{wait_hour}小时了还没有人回复",
    "每次打电话都要等好久，你们客服都是摆设吗",
    "上次的问题说好了2天解决，现在一周了还没消息",
]

_REASONS_VALUE = [
    "流量根本用不完，每个月就用了{used_gb}G，太浪费了",
    "套餐里的通话分钟根本不够用",
    "送的流量太少了，我每个月要超过{extra_gb}G",
    "这个套餐完全不适合我，我根本不看视频",
]

_REASONS_COMPETITOR = [
    "我朋友{competitor}{comp_amount}还送视频会员",
    "人家{competitor}信号比你们好多了",
    "{competitor}的套餐便宜而且网速快",
    "已经买了{competitor}的卡了，这个号不要了",
    "{competitor}那边{comp_amount}的套餐比你们划算太多",
]

_REASONS_MOVE = [
    "我搬家了，现在在{location}，但是号码是{old_location}的",
    "换工作了，从{old_location}搬到了{location}，漫游费用太高了",
    "我是{old_location}的号码，现在长住{location}了",
]

_ANGRY_PHRASES = [
    "你们就是坑钱的！我要投诉到工信部去！",
    "你们太过分了！这就是明着抢钱！",
    "我再也不相信你们了！",
    "你们这样我要曝光到网上去！",
    "我要打12315投诉你们！",
    "你们不是第一次这样了，每次都这样骗钱！",
    "我要去消协告你们！",
]

_CALM_PHRASES = [
    "好吧，那你帮我看看吧。",
    "可以，那我考虑一下。",
    "嗯，了解了，谢谢。",
    "好的，那麻烦你了。",
    "行，那我再看看。",
]

_CONFUSED_PHRASES = [
    "我不太明白，你能再解释一下吗？",
    "这是怎么扣的？我从来没开过这个啊？",
    "我查了半天也没搞明白是怎么回事。",
    "我没订过这个服务啊，是你们自己给我加的吧？",
]

# ── Full scenario templates ──

_TEMPLATES = {
    "转网流失": {
        "weight": 20,
        "tags": ["转网/携号转网", "high"],
        "templates": [
            (
                "客服: {greeting}\n"
                "用户: 我要转网！{reason_signal}，{reason_competitor}，我要携号转网！{angry}"
            ),
            (
                "客服: {greeting}\n"
                "用户: 你好，我想办携号转网。\n"
                "客服: 请问您为什么要转网呢？\n"
                "用户: {reason_price}，而且{reason_signal}。\n"
                "客服: 了解了，我帮您看看有没有更合适的套餐……\n"
                "用户: {calm}"
            ),
            (
                "客服: {greeting}\n"
                "用户: 我想问一下携号转网怎么办理。{reason_move}\n"
                "客服: 请问您是要转到哪家运营商呢？\n"
                "用户: {competitor}，他们那边便宜多了，而且{reason_signal}。\n"
                "客服: 我帮您记录一下……\n"
                "用户: {confused}"
            ),
            (
                "客服: {greeting}\n"
                "用户: 我要转网！{reason_price}，{reason_service}，{angry}\n"
                "客服: 不好意思给您带来不便，我帮您看看有没有优惠方案……\n"
                "用户: 不用了，我已经决定了！帮我办转网！"
            ),
        ],
    },
    "投诉扣费": {
        "weight": 15,
        "tags": ["投诉", "high"],
        "templates": [
            (
                "客服: {greeting}\n"
                "用户: 我要投诉！你们乱扣我费用！上个月多扣了我{extra_charge}块钱！\n"
                "客服: 不好意思，您能说具体是什么扣费吗？\n"
                "用户: 我查账单发现有个什么{random_service}业务，我从来没开过！{angry}\n"
                "客服: 我帮您查一下业务开通记录……\n"
                "用户: 你们要是不解决，我就投诉到工信部去！"
            ),
            (
                "客服: {greeting}\n"
                "用户: 你看一下我的账单，莫名其妙多了{extra_charge}块钱的扣费。\n"
                "客服: 我帮您查询一下……哦，这边显示您开通了{random_service}会员。\n"
                "用户: 不可能！我根本没开过，是不是你们偷偷给我加的？{angry}"
            ),
            (
                "客服: {greeting}\n"
                "用户: 我要投诉！你们这个月扣了我两次话费！\n"
                "客服: 不好意思，请问是什么时候扣的？\n"
                "用户: 3号扣了一次，15号又扣了一次，每次{extra_charge}块，你们想干嘛？{angry}"
            ),
        ],
    },
    "投诉服务": {
        "weight": 10,
        "tags": ["投诉", "high"],
        "templates": [
            (
                "客服: {greeting}\n"
                "用户: 你们服务太差了！{reason_service}\n"
                "客服: 非常抱歉，请问是什么问题呢？\n"
                "用户: 我之前投诉{reason_signal}，你们说24小时回复，现在都过了好几天了！{angry}"
            ),
            (
                "客服: {greeting}\n"
                "用户: 我要投诉你们的营业厅，{location}那个营业厅工作人员态度极差！\n"
                "客服: 不好意思，请问具体是什么情况？\n"
                "用户: 我去办业务，排了快一个小时，轮到我了他就说要下班了不办了！{angry}"
            ),
        ],
    },
    "投诉网速": {
        "weight": 8,
        "tags": ["投诉", "high"],
        "templates": [
            (
                "客服: {greeting}\n"
                "用户: 我要投诉！你们承诺的5G网速根本达不到！\n"
                "客服: 请问您在什么位置测速的呢？\n"
                "用户: 在{location}这边，测速才{mb}M，你们宣传说千兆，这不是虚假宣传吗？{angry}"
            ),
            (
                "客服: {greeting}\n"
                "用户: 你们网速太差了，{location}看个1080p的视频都卡！\n"
                "客服: 请问您是什么套餐呢？\n"
                "用户: {amount}的套餐，号称5G极速，结果就这？{angry}"
            ),
        ],
    },
    "降套餐": {
        "weight": 10,
        "tags": ["降套餐", "medium"],
        "templates": [
            (
                "客服: {greeting}\n"
                "用户: 你好，我想问一下我的套餐能不能降档。\n"
                "客服: 请问您现在是什么套餐呢？\n"
                "用户: {amount}的套餐，但是{reason_value}。\n"
                "客服: 我帮您看看有没有更合适的套餐……\n"
                "用户: {calm}"
            ),
            (
                "客服: {greeting}\n"
                "用户: 你好，我想换个便宜点的套餐，现在的太贵了。\n"
                "客服: 好的，您现在套餐是多少钱的呢？\n"
                "用户: {amount}，每个月都用不了那么多，{reason_value}。\n"
                "客服: 我推荐您办{lower_amount}的套餐，包含{lower_gb}G流量和{lower_min}分钟通话。\n"
                "用户: 可以，那帮我换吧。"
            ),
        ],
    },
    "销户": {
        "weight": 8,
        "tags": ["销户", "medium"],
        "templates": [
            (
                "客服: {greeting}\n"
                "用户: 我要注销号码。\n"
                "客服: 请问您为什么要注销呢？\n"
                "用户: {reason_competitor}，{reason_signal}。\n"
                "客服: 您现在的套餐是{amount}的对吗？我们可以帮您调整……\n"
                "用户: 算了，我已经买了{competitor}的卡了，这个号不要了。"
            ),
            (
                "客服: {greeting}\n"
                "用户: 你好，帮我销户。\n"
                "客服: 好的，注销前请确认账户没有欠费。\n"
                "用户: 我查过了，没有欠费。{reason_price}，{reason_service}。\n"
                "客服: 了解了，我帮您办理……"
            ),
        ],
    },
    "业务咨询": {
        "weight": 8,
        "tags": ["业务咨询", "low"],
        "templates": [
            (
                "客服: {greeting}\n"
                "用户: 你好，我想了解一下你们有什么套餐。\n"
                "客服: 我们的套餐有很多种，您主要是什么需求呢？\n"
                "用户: 我主要上网比较多，通话不太多。\n"
                "客服: 那我推荐我们的畅享套餐，{recommend_amount}元每月含{recommend_gb}G流量。\n"
                "用户: 听起来不错，{calm}"
            ),
            (
                "客服: {greeting}\n"
                "用户: 你好，我想问一下{random_service}怎么开通。\n"
                "客服: 您可以在App上直接开通，或者我这边帮您办也行。\n"
                "用户: 那你帮我办吧。\n"
                "客服: 好的，{random_service}月费{service_fee}元，确认开通吗？\n"
                "用户: 可以，开通。"
            ),
        ],
    },
    "合约续约": {
        "weight": 8,
        "tags": ["业务办理", "low"],
        "templates": [
            (
                "客服: {greeting}\n"
                "用户: 你好，我的合约快到期了，想续约。\n"
                "客服: 好的，请问您的手机号码是？\n"
                "用户: {phone}\n"
                "客服: 帮您查到了，您的合约还有{remain_months}个月到期。现在续约可以享受优惠。\n"
                "用户: 有什么优惠？\n"
                "客服: 预存{deposit}送{deposit}，而且套餐费打{discount}折。\n"
                "用户: 那还不错，帮我办吧。"
            ),
            (
                "客服: {greeting}\n"
                "用户: 你好，我想续约我的套餐。\n"
                "客服: 好的，您现在{amount}的套餐，马上到期了。续约可以升级到{upgrade_amount}套餐，多送{extra_gb}G流量。\n"
                "用户: 那价格呢？\n"
                "客服: 续约前6个月打{discount}折，相当于每月{discounted_amount}。\n"
                "用户: 行，那续吧。"
            ),
        ],
    },
    "流量咨询": {
        "weight": 6,
        "tags": ["业务咨询", "low"],
        "templates": [
            (
                "客服: {greeting}\n"
                "用户: 你好，我的流量用完了，想买流量包。\n"
                "客服: 您可以买{extra_gb}G流量包，{flow_price}元。\n"
                "用户: 有没有便宜一点的？\n"
                "客服: 还有{cheap_gb}G的{cheap_flow_price}元。\n"
                "用户: 那买{cheap_gb}G的吧。"
            ),
            (
                "客服: {greeting}\n"
                "用户: 你好，我想问一下怎么查剩余流量。\n"
                "客服: 您可以发送短信10086查询，或者在App上查看。\n"
                "用户: 好的，另外我流量快用完了，有什么流量包可以买？\n"
                "客服: 推荐您办理流量月包，{flow_price}元{extra_gb}G。\n"
                "用户: 好的，帮我开一个。"
            ),
        ],
    },
    "宽带升级": {
        "weight": 5,
        "tags": ["业务办理", "low"],
        "templates": [
            (
                "客服: {greeting}\n"
                "用户: 你好，我想升级宽带，现在{old_speed}M，想升到{new_speed}M。\n"
                "客服: {new_speed}M的宽带月费{bb_price}元。\n"
                "用户: 有点贵，能不能便宜点？\n"
                "客服: 宽带+手机套餐一起办的话，月费只要{bb_bundle_price}。\n"
                "用户: 那还行，帮我办吧。"
            ),
        ],
    },
    "老人套餐": {
        "weight": 5,
        "tags": ["业务咨询", "low"],
        "templates": [
            (
                "客服: {greeting}\n"
                "用户: 你好，我想给爸妈办个适合老人的套餐，他们不会用流量，主要打电话。\n"
                "客服: 我们有孝心套餐，月费{senior_price}元，含{senior_min}分钟通话。\n"
                "用户: 这个不错，没有其他费用吧？\n"
                "客服: 没有隐藏费用。\n"
                "用户: 那帮我办一个，{calm}"
            ),
        ],
    },
    "国际漫游": {
        "weight": 5,
        "tags": ["业务咨询", "low"],
        "templates": [
            (
                "客服: {greeting}\n"
                "用户: 你好，我下个月要去{country}出差，想了解一下国际漫游费用。\n"
                "客服: {country}漫游流量是{roam_fee}元/天，通话{roam_call_fee}元/分钟。\n"
                "用户: 有点贵，有没有套餐可以包？\n"
                "客服: 有{roam_days}天国际漫游包，{roam_pkg}元。\n"
                "用户: 那可以，帮我开一个。"
            ),
        ],
    },
}

# ── Random fillers ──

_COUNTRIES = ["日本", "美国", "新加坡", "韩国", "泰国", "英国", "法国", "澳大利亚"]

_RANDOM_SERVICES = [
    "视频彩铃", "云盘会员", "咪咕音乐", "5G增强包", "安全卫士",
    "来电管家", "和彩云", "游戏加速", "流量安心包", "视频会员包",
]

_CALLEE_NAMES = [
    "张先生", "李女士", "王先生", "赵女士", "刘先生", "陈女士",
    "黄先生", "周女士", "吴先生", "林女士",
]


def _fill_placeholders(template: str) -> str:
    """Fill placeholders in a template with randomized values."""
    # Phone
    template = template.replace(
        "{phone}",
        random.choice(_PHONE_PREFIXES) + "".join(str(random.randint(0, 9)) for _ in range(8)),
    )

    # Location
    template = template.replace("{location}", random.choice(_LOCATIONS))
    old_loc = random.choice(_LOCATIONS)
    while old_loc in template:
        old_loc = random.choice(_LOCATIONS)
    template = template.replace("{old_location}", old_loc)

    # Competitor
    comp = random.choice(_COMPETITORS)
    template = template.replace("{competitor}", comp)

    # Amounts
    amount = random.choice([99, 129, 139, 159, 189, 199, 238, 269, 299])
    template = template.replace("{amount}", str(amount))
    comp_amount = random.choice([59, 79, 89, 99, 109, 129])
    template = template.replace("{comp_amount}", str(comp_amount))
    lower_amount = random.choice([39, 59, 69, 79])
    template = template.replace("{lower_amount}", str(lower_amount))
    extra_charge = random.choice([30, 50, 68, 88, 99, 120])
    template = template.replace("{extra_charge}", str(extra_charge))

    # Data
    used_gb = random.choice([5, 10, 15, 20, 30])
    template = template.replace("{used_gb}", str(used_gb))
    extra_gb = random.choice([5, 10, 20, 30, 50])
    template = template.replace("{extra_gb}", str(extra_gb))
    lower_gb = random.choice([10, 15, 20, 30])
    template = template.replace("{lower_gb}", str(lower_gb))
    recommend_gb = random.choice([30, 50, 60, 80, 100])
    template = template.replace("{recommend_gb}", str(recommend_gb))
    cheap_gb = random.choice([1, 3, 5])
    template = template.replace("{cheap_gb}", str(cheap_gb))

    # Minutes
    lower_min = random.choice([100, 200, 300, 500])
    template = template.replace("{lower_min}", str(lower_min))
    senior_min = random.choice([200, 300, 500, 1000])
    template = template.replace("{senior_min}", str(senior_min))

    # Speed
    mb = random.choice([5, 10, 20, 30, 50])
    template = template.replace("{mb}", str(mb))
    old_speed = random.choice([50, 100])
    template = template.replace("{old_speed}", str(old_speed))
    new_speed = random.choice([300, 500, 1000])
    template = template.replace("{new_speed}", str(new_speed))

    # Wait time
    wait = random.choice([5, 10, 15, 20, 30])
    template = template.replace("{wait}", str(wait))
    wait_hour = random.choice([24, 36, 48, 72])
    template = template.replace("{wait_hour}", str(wait_hour))

    # Contract
    remain_months = random.choice([1, 2, 3])
    template = template.replace("{remain_months}", str(remain_months))
    deposit = random.choice([100, 200, 300])
    template = template.replace("{deposit}", str(deposit))
    discount = random.choice([5, 6, 7, 8])
    template = template.replace("{discount}", str(discount))
    upgrade_amount = random.choice([199, 239, 269, 299])
    template = template.replace("{upgrade_amount}", str(upgrade_amount))
    discounted_amount = max(1, int(upgrade_amount * discount / 10))
    template = template.replace("{discounted_amount}", str(discounted_amount))

    # Flow
    flow_price = random.choice([10, 15, 20, 30])
    template = template.replace("{flow_price}", str(flow_price))
    cheap_flow_price = random.choice([3, 5, 8])
    template = template.replace("{cheap_flow_price}", str(cheap_flow_price))

    # Broadband
    bb_price = random.choice([99, 129, 159, 189])
    template = template.replace("{bb_price}", str(bb_price))
    bb_bundle_price = random.choice([59, 79, 99])
    template = template.replace("{bb_bundle_price}", str(bb_bundle_price))

    # Senior
    senior_price = random.choice([19, 29, 39, 59])
    template = template.replace("{senior_price}", str(senior_price))

    # Recommend
    recommend_amount = random.choice([59, 79, 99, 129, 159])
    template = template.replace("{recommend_amount}", str(recommend_amount))

    # Service
    random_service = random.choice(_RANDOM_SERVICES)
    template = template.replace("{random_service}", random_service)
    service_fee = random.choice([5, 10, 15, 20])
    template = template.replace("{service_fee}", str(service_fee))

    # Roaming
    country = random.choice(_COUNTRIES)
    template = template.replace("{country}", country)
    roam_fee = random.choice([25, 30, 35, 50])
    template = template.replace("{roam_fee}", str(roam_fee))
    roam_call_fee = random.choice(["0.99", "1.99", "2.99"])
    template = template.replace("{roam_call_fee}", roam_call_fee)
    roam_days = random.choice([3, 5, 7, 10])
    template = template.replace("{roam_days}", str(roam_days))
    roam_pkg = random.choice([68, 88, 128, 168])
    template = template.replace("{roam_pkg}", str(roam_pkg))

    # Greetings / sentiment phrases
    template = template.replace("{greeting}", random.choice(_GREETINGS))
    template = template.replace("{angry}", random.choice(_ANGRY_PHRASES))
    template = template.replace("{calm}", random.choice(_CALM_PHRASES))
    template = template.replace("{confused}", random.choice(_CONFUSED_PHRASES))

    # Reason sub-templates
    template = template.replace("{reason_price}", random.choice(_REASONS_PRICE))
    template = template.replace("{reason_signal}", random.choice(_REASONS_SIGNAL))
    template = template.replace("{reason_service}", random.choice(_REASONS_SERVICE))
    template = template.replace("{reason_value}", random.choice(_REASONS_VALUE))
    template = template.replace("{reason_competitor}", random.choice(_REASONS_COMPETITOR))
    template = template.replace("{reason_move}", random.choice(_REASONS_MOVE))

    return template


def _random_amount() -> int:
    return random.choice([99, 129, 139, 159, 189, 199, 238, 269, 299])


def _random_comp_amount() -> int:
    return random.choice([59, 79, 89, 99, 109, 129])


class CallGenerator:
    """Random call transcript generator."""

    def __init__(self):
        # Build weighted scenario list for random selection
        self._weighted_scenarios = []
        for name, cfg in _TEMPLATES.items():
            self._weighted_scenarios.extend([name] * cfg["weight"])

    def generate_batch(self, count: int) -> dict:
        """Generate *count* random call transcripts.

        Returns a dict mapping call_id → transcript suitable for batch API.
        """
        result = {}
        base_ts = int(time.time() * 1000)
        for i in range(count):
            call_id = "gen_{}_{}".format(base_ts, i)
            result[call_id] = self.generate_one()
        return result

    def generate_one(self) -> str:
        """Generate a single random call transcript."""
        scenario = random.choice(self._weighted_scenarios)
        cfg = _TEMPLATES[scenario]
        template = random.choice(cfg["templates"])
        return _fill_placeholders(template)

    def generate_simulation_calls(self, count: int) -> list:
        """Generate a list of dicts suitable for simulation push.

        Returns list of {call_id, customer, transcript}.
        """
        base_ts = int(time.time() * 1000)
        calls = []
        for i in range(count):
            call_id = "sim_{}_{}".format(base_ts, i)
            customer = "{}****{}".format(
                random.choice(_PHONE_PREFIXES),
                str(random.randint(1000, 9999)),
            )
            calls.append({
                "call_id": call_id,
                "customer": customer,
                "transcript": self.generate_one(),
            })
        return calls


# Singleton
call_generator = CallGenerator()
