# FX Lab プロジェクト バックアップスクリプト
# 実行方法: ./backup.ps1 "コミットメッセージ"

param (
    [string]$Message = "Update project: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
)

Write-Host "--- バックアップを開始します ---" -ForegroundColor Cyan

# 1. 変更をステージング
Write-Host "1. 変更をステージング中..."
git add .

# 2. コミット
Write-Host "2. コミット中: $Message"
git commit -m "$Message"

# 3. プッシュ
Write-Host "3. GitHubへプッシュ中..."
git push origin master:main

if ($LASTEXITCODE -eq 0) {
    Write-Host "--- バックアップが成功しました！ ---" -ForegroundColor Green
} else {
    Write-Host "--- バックアップ中にエラーが発生しました ---" -ForegroundColor Red
}
