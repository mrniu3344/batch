### 安装依赖

```bash
pip install -r requirements.txt
```

### 数据库配置

请确保 PostgreSQL 数据库已正确配置，并更新相应的连接参数。

# 環境構築

python 3.12 インストール

##上記インストールしたら,requirements をインストール
ec2:
pip install -r requirements.txt

# 起動

## スケジューラー機能について

このアプリケーションは`schedule`ライブラリを使用して、pm2 で常時起動し、Python 内でスケジューリングを行います。

- **実行時間**: 毎日 19:00 に自動実行
- **開発環境**: 1 分間隔でテスト実行（mode=dev の場合）
- **本番環境**: 毎日 19:00 のみ実行（mode=prd の場合）

## ec2

### 本番環境

```bash
pm2 start pm2PrdDaily.json
```

### 開発環境

```bash
pm2 start pm2DevDaily.json
```

## local 开发时可以这样执行

```bash
python midnight_batch.py -m dev
python midnight_batch.py -m dev -t 2026/01/01
```

## ec2

sudo systemctl start redis6

## ログ確認

```bash
# 本番環境のログ
pm2 logs prd-midnight_batch

# 開発環境のログ
pm2 logs dev-midnight_batch
```
