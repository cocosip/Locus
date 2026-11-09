# GitHub Actions CI/CD 配置说明

## 统一约定

Locus 与 obconnector-dotnet 使用同一套 CI/CD 约定（差异只在项目特定的构建/校验步骤）：

| 项目 | 约定 |
| --- | --- |
| 触发 | 默认分支 push、指向默认分支的 PR → 编译 + 测试；版本 tag push → 编译 + 测试 + 打包 + 发布 |
| 不触发 | 其他分支（含 `dev`）、非版本 tag |
| 版本 | 项目属性 `<Version>` 是唯一来源；tag 必须等于 `v<Version>` 或 `<Version>`，且指向 HEAD |
| 编译次数 | 每次运行只 restore / build 一次，测试与打包复用同一份 Release 输出 |
| 发布 | 发布 job 只下载构建产物：不 checkout 源码、不 restore、不编译 |
| 交付物 | 打包与校验在编译 job 内完成，产物作为 workflow artifact 传给发布 job |
| 凭证 | NuGet Trusted Publishing（GitHub OIDC 换 1 小时临时 API Key），无长期密钥 |

## 触发范围

| 事件 | 分支 / 引用 | 执行内容 |
| --- | --- | --- |
| `push` | `master` | 编译 + 测试 → 打包 + 包内容校验（不发布） |
| `pull_request` | 目标分支 `master` | 编译 + 测试（不打包） |
| `push` | 版本 tag `v2.1.0` 或 `2.1.0` | 编译 + 测试 + 打包 + 发布 NuGet / GitHub Release |
| `workflow_dispatch` | 手动 | 同 PR：编译 + 测试 |
| 其他 | `dev`、`feature/**` 等 | 不触发任何任务 |

> 在 `master` push 上就打包并校验，是为了在打 tag 之前证明“可发布包”本身是好的；真正的发布只发生在版本 tag 上。

## Workflow 说明

### 1. build-and-test（构建、测试、打包）

1. 检出代码（`fetch-depth: 0`，发布说明需要完整 tag 历史）
2. 设置 .NET 8.0 / 9.0 / 10.0
3. 恢复 NuGet 依赖（带包缓存）
4. **解析版本并校验 tag**：读取 `common.props` 的 `<Version>`；tag 必须与之一致且指向 HEAD，否则在编译前失败
5. 编译 `Locus.slnx`（Release，**本次运行唯一一次编译**）
6. 运行 `net8.0` / `net9.0` / `net10.0` 全部测试（`--no-build` 复用上一步产物），收集覆盖率
7. 上传测试结果（TRX）与覆盖率报告
8. `master` / tag：打包 `Locus` 包（复用编译输出），一次 `pack` 同时产出 `.nupkg` 与 `.snupkg`（`common.props` 已启用 `IncludeSymbols` / `SymbolPackageFormat=snupkg`）
9. `master` / tag：校验包内容——四个 TFM 的依赖组与 `Locus.dll`、符号包存在、包内无测试/覆盖率残留
10. tag：生成 `RELEASE_NOTES.md`（上一个版本 tag 到当前 tag 的非 merge 提交、对比链接、安装命令）
11. `master` / tag：上传 `nuget-packages` artifact（包 + 发布说明）

### 2. publish（发布）

只在 tag push 且 `build-and-test` 成功时运行，权限限定为 `contents: write` + `id-token: write`：

1. 设置 .NET（仅用于 `dotnet nuget push`）
2. 下载 `nuget-packages` artifact
3. 通过 OIDC 换取临时 API Key
4. 推送 `.nupkg`（同目录 `.snupkg` 自动一起推送）
5. 用 `gh release create --verify-tag --notes-file` 创建 GitHub Release（含版本说明与包文件）；预发布版本自动标记 `--prerelease`

## 配置步骤

### 1. 配置 NuGet Trusted Publishing

发布使用 NuGet 的 Trusted Publishing，不再需要长期 API Key：

1. 登录 [NuGet.org](https://www.nuget.org/)
2. 点击用户名 → **Trusted Publishing** → **Add new policy**
3. 填写：
   - **Repository owner**: `cocosip`
   - **Repository**: `Locus`
   - **Workflow file**: `ci-cd.yml`（只填文件名，不带 `.github/workflows/`）
   - **Environment**: 留空（当前 workflow 未使用 GitHub Environment）
4. 工作流中的 `NuGet/login@v1` 使用 `user: virtual`（nuget.org 的 profile 名，不是邮箱）
5. 无需配置 `NUGET_API_KEY` secret

参考：[NuGet Trusted Publishing - GitHub Actions](https://learn.microsoft.com/zh-cn/nuget/nuget-org/trusted-publishing#github-actions)

### 2. 更新项目元数据

编辑根目录的 `common.props` 文件，更新以下信息：

```xml
<Authors>Your Name or Organization</Authors>
<Company>Your Company</Company>
<PackageProjectUrl>https://github.com/yourusername/Locus</PackageProjectUrl>
<RepositoryUrl>https://github.com/yourusername/Locus</RepositoryUrl>
<Version>2.1.0</Version>
```

### 3. 配置分支保护 (可选但推荐)

1. 进入 Settings → Branches
2. 为 `master` 添加规则
3. 启用：
   - Require status checks to pass before merging
   - Require branches to be up to date before merging
   - 选择 **Build and Test** 作为必需检查

## 发布新版本

1. **更新版本号**：修改 `common.props` 的 `<Version>`（例如 `2.2.0`），提交并推送到 `master`
2. **等待 `master` 的构建与测试通过**（此时已打包并校验）
3. **创建并推送同名 tag**：
   ```bash
   git tag v2.2.0
   git push origin v2.2.0
   ```
   tag 与 `common.props` 不一致、或 tag 不指向 HEAD 时，流水线会在编译前直接失败。
4. **自动化流程**：编译 + 测试 → 打包 → 发布 NuGet.org → 创建 GitHub Release
5. **验证**：检查 Actions 运行状态、Releases 页面与 NuGet.org 包页面

## 版本号规范

- **MAJOR**：不兼容的 API 更改
- **MINOR**：向后兼容的功能新增
- **PATCH**：向后兼容的问题修复

预发布版本（如 `v2.2.0-rc.1`）会创建 GitHub prerelease，并在 NuGet 上标记为预发布。

## 发布说明

发布说明在构建 job 中由 Git 历史生成并作为 artifact 传递：

- 非 merge 提交列表，格式 `- 短哈希 提交信息 (作者)`
- 与上一个版本 tag 的对比链接
- `dotnet add package Locus --version <版本>` 安装命令

因此建议使用约定式提交信息，例如 `feat: add directory quota management`。

## 故障排查

### 测试失败
- 在本地运行 `dotnet test Locus.slnx` 重现问题
- 修复后推送到 `master`（或 PR 到 `master`）自动重跑

### 打包失败
- 检查 `common.props` 与各 `csproj` 的包元数据
- 确认 tag 与 `<Version>` 一致
- 注意：本包的 `pack` 刻意不加 `--no-build`——自定义 target 把 4 个子程序集嵌入包内，项目引用场景下 SDK 会以 `NETSDK1085` 拒绝 `NoBuild=true`；而 `--no-build -p:BuildProjectReferences=false` 虽能通过，却会静默丢掉子程序集的 `.xml`/`.pdb`。增量检查约 3 秒，不会重新编译。

### 发布到 NuGet 失败
- 确认 nuget.org 的 Trusted Publishing 策略为 `cocosip` / `Locus` / `ci-cd.yml`
- 确认 publish job 有 `id-token: write`
- 版本已存在时 `--skip-duplicate` 会跳过；需要新版本请更新 tag

### GitHub Release 创建失败
- 确认 `contents: write` 权限
- 确认 tag 已推送且未被其他 Release 占用

## 相关链接

- [GitHub Actions 文档](https://docs.github.com/en/actions)
- [NuGet 打包文档](https://docs.microsoft.com/en-us/nuget/create-packages/)
- [语义化版本规范](https://semver.org/)
