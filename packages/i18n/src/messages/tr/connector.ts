import type { PartialTranslation } from "../../message";
import type { messages as source } from "../en/connector";

export const messages = {
  // Sunucu ayarları > Bağlayıcılar: bu bilgisayarın yerleşik GitHub bağlantısı.
  "connector.github.title": "GitHub",
  "connector.github.description":
    "Bu bilgisayardaki her ajan GitHub depolarınızı, sorunlarınızı (issues), çekme isteklerinizi (pull requests), gh ve git araçlarını kullanabilir.",
  "connector.github.connect": "GitHub'a Bağlan",
  "connector.github.pendingTitle": "Bu kodu GitHub'a yazın",
  "connector.github.pendingDescription": "GitHub tarayıcınızda açıldı. Bağlanmak için kodu oraya yazın.",
  "connector.github.waiting": "GitHub bekleniyor",
  "connector.github.copyCode": "Kodu kopyala",
  "connector.github.codeCopied": "Kopyalandı",
  "connector.github.openGitHub": "GitHub'ı Aç",
  "connector.github.cancel": "İptal",
  "connector.github.connectedAs": "@{login} olarak bağlandı",
  "connector.github.repositoriesTitle": "Depolar",
  "connector.github.repositoriesDescription":
    "Ajanlar yalnızca OpenBot GitHub Uygulamasının yüklü olduğu depoları kullanabilir.",
  "connector.github.chooseRepositories": "Depoları seç",
  "connector.github.repositoriesLoading": "Depolar GitHub'dan okunuyor",
  "connector.github.repositoriesFailed": "OpenBot depoları GitHub'dan okuyamadı.",
  "connector.github.noRepositories": "OpenBot GitHub Uygulaması henüz bir depoya yüklenmedi.",
  // {count} bir sayıdır (örneğin 12).
  "connector.github.moreRepositories": {
    one: "Ve {count} depo daha",
    other: "Ve {count} depo daha",
  },
  // Yalnızca üyelerinin görebileceği bir deponun yanındaki rozet.
  "connector.github.private": "Gizli",
  "connector.github.disconnect": "Bağlantıyı Kes",
  "connector.github.disconnectTitle": "GitHub Bağlantısını Kes",
  "connector.github.disconnectSummary": "Tüm ajanlar GitHub erişimini kaybeder. Sohbetleriniz ve dosyalarınız kalır.",
  "connector.github.expiredTitle": "GitHub bağlantısının süresi doldu",
  "connector.github.expiredDescription": "Ajanlara yeniden GitHub erişimi vermek için @{login} olarak tekrar bağlanın.",
  "connector.github.reconnect": "Yeniden Bağlan",
  "connector.github.actionFailed": "OpenBot GitHub bağlantısını değiştiremedi.",
  // GitHub sayfasının üst kısmındaki adın yanındaki durum.
  "connector.github.statusConnected": "Bağlandı",
  "connector.github.statusConnecting": "Bağlanıyor",
  "connector.github.statusExpired": "Süresi doldu",
  "connector.github.statusNotSetUp": "Ayarlanmadı",
  "connector.github.accountTitle": "Hesap",
  // {count} listedeki depo sayısıdır (örneğin 12).
  "connector.github.filterPlaceholder": {
    one: "{count} depoyu filtrele",
    other: "{count} depoyu filtrele",
  },
  "connector.github.filterLabel": "Depoları filtrele",
  // {query} kullanıcının filtreye yazdığı metindir.
  "connector.github.noMatch": "“{query}” ile eşleşen depo bulunamadı.",
  // Bağlanma iletişim kutusu. Adımlar sayı olarak gösterilir; ekran okuyucular adları okur.
  "connector.github.stepSignIn": "Giriş yap",
  "connector.github.stepConnected": "Bağlandı",
  "connector.github.requestingCode": "OpenBot GitHub'dan bir kod istiyor.",
  // {code} GitHub'a yazılacak koddur (örneğin WDJB-MJHT).
  "connector.github.codeLabel": "Kod {code}",
  "connector.github.failedTitle": "GitHub bağlanamadı",
  "connector.github.cancelConnecting": "GitHub bağlantısını iptal et",
  // {count} ajanların kullanabileceği depo sayısıdır.
  "connector.github.connectedSummary": {
    one: "{count} depo · bu bilgisayardaki her ajan",
    other: "{count} depo · bu bilgisayardaki her ajan",
  },
  "connector.github.done": "Bitti",
  "connector.github.later": "Daha sonra",
  // Bağlantıyı kesme öncesi onay. {login} GitHub hesap adıdır (örneğin octocat).
  "connector.github.disconnectConfirmTitle": "GitHub bağlantısı kesilsin mi?",
  "connector.github.disconnectConfirmDescription": "Bağlantıyı kesmek @{login} oturumunu bu bilgisayardan kaldırır.",
  "connector.github.disconnectEffectTools": "Tüm ajanlar GitHub araçlarını, gh ve git erişimini kaybeder.",
  "connector.github.disconnectEffectRevoke": "Ardından OpenBot yetkisini kaldırabileceğiniz GitHub açılır.",
  "connector.github.disconnectEffectKept": "Sohbetler, dosyalar ve ajan hafızası bu bilgisayarda kalır.",
  "connector.github.keepConnected": "Bağlı kal",
  "connector.github.close": "Kapat",
  // Bir entegrasyon sayfasının onu kaldıran eylemi içeren son bölümü.
  "connector.dangerZone": "Tehlikeli bölge",
} as const satisfies PartialTranslation<typeof source>;
