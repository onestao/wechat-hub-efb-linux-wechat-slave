from pathlib import Path

from setuptools import find_packages, setup


ROOT = Path(__file__).parent
VERSION_NS = {}
exec((ROOT / "efb_wechat_comwechat_slave" / "__version__.py").read_text(encoding="utf-8"), VERSION_NS)


setup(
    name="efb-linux-wechat-slave",
    version=VERSION_NS["__version__"],
    description="EH Forwarder Bot slave for Linux WeChat via WeChat Core HTTP API",
    long_description=(ROOT / "README.md").read_text(encoding="utf-8"),
    long_description_content_type="text/markdown",
    author="honus; Linux WeChat Hub contributors",
    url="https://github.com/ehForwarderBot/efb-wechat-comwechat-slave",
    packages=find_packages(exclude=["tests", "tests.*"]),
    python_requires=">=3.9",
    keywords=["wechat", "ehforwarderbot", "linux"],
    install_requires=[
        "ehforwarderbot>=2.0.0",
        "requests>=2.28",
        "PyYAML>=6.0",
    ],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Communications :: Chat",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3 :: Only",
        "Operating System :: OS Independent",
    ],
    entry_points={
        "ehforwarderbot.slave": "wechat.linux = efb_wechat_comwechat_slave:LinuxWeChatChannel",
    },
)
