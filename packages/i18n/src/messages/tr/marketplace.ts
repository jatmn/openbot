import type { PartialTranslation } from "../../message";
import type { messages as source } from "../en/marketplace";

export const messages = {
  "marketplace.category.coding": "Kodlama",
  "marketplace.category.design": "Tasarım",
  "marketplace.category.dataAnalytics": "Veri ve Analitik",
  "marketplace.category.documents": "Belgeler",
  "marketplace.category.productivity": "Verimlilik",
  "marketplace.category.research": "Araştırma",
  "marketplace.category.automation": "Otomasyon",
  "marketplace.category.other": "Diğer",
  "marketplace.loadFailed": "Pazaryeri yüklenemedi.",
  "marketplace.loading.skills": "Beceriler yükleniyor",
  "marketplace.loading.agents": "Ajanlar yükleniyor",
  "marketplace.noMatch.skills": "Bu aramayla eşleşen beceri yok.",
  "marketplace.noMatch.agents": "Bu aramayla eşleşen ajan yok.",
  "marketplace.loadMore": "Daha fazla yükle",
  "marketplace.version": "Sürüm {version}",

  "marketplace.title": "Pazaryeri",
  "marketplace.close": "Pazaryerini kapat",
  "marketplace.kinds": "Pazaryeri içerik türleri",
  "marketplace.tab.agents": "Ajanlar",
  "marketplace.tab.skills": "Beceriler",

  "marketplace.plugins.missing": "Bu eklenti OpenBot kataloğunda yok.",

  "marketplace.agents.loadingDetail": "Ajan ayrıntıları yükleniyor…",
  "marketplace.agents.skills": "Beceriler",
  "marketplace.agents.routines": "Rutinler",
  "marketplace.agents.routineActive": "Etkin",
  "marketplace.agents.routineInactive": "Devre dışı",

  "marketplace.skill.loading": "Beceri yükleniyor",
  "marketplace.skill.update": "Beceriyi güncelle",
  "marketplace.try.readFailed": "OpenBot bu ajanın becerilerini okuyamadı. Tekrar deneyin.",
  "marketplace.try.enable": "Bu beceriyi denemek için ajan ayarlarından etkinleştirin.",
  "marketplace.try.repair": "Bu beceriyi denemek için ajan ayarlarından onarın.",
  "marketplace.try.update": "Bu sürümü denemek için bu beceriyi güncelleyin.",
  "marketplace.try.composerUnavailable": "Ajan mesaj kutusu kullanılamıyor.",

  "marketplace.error.openLink": "Bağlantı açılamadı.",
  "marketplace.error.copyLink": "Bağlantı kopyalanamadı.",
  "marketplace.error.connectNoServer": "Bu uygulamayı bağlamak için yerel bir sunucu seçin.",
  "marketplace.error.installNoServer": "Bir eklenti yüklemek için yerel bir sunucu seçin.",
  "marketplace.error.installNoAgent": "Bu eklentinin becerilerini yüklemek için bir ajan seçin.",
  "marketplace.error.installLocalOnHost":
    "{name} uygulamasını bu ajanları çalıştıran bilgisayara yükleyin: uygulaması sunucusunu o bilgisayarda çalıştırır.",
  "marketplace.error.installOnHost":
    "{name} uygulamasını bu ajanları çalıştıran bilgisayara yükleyin: uygulaması bir tarayıcı girişi gerektirir.",
  "marketplace.error.appInvalid": "{name} eklenemiyor: {reason}",
  "marketplace.error.uninstallNoServer": "Bir eklentiyi kaldırmak için yerel bir sunucu seçin.",
  "marketplace.error.uninstallPartial": "{name} uygulamasının bazı kısımları kaldırılamadı. {failures}",
  "marketplace.error.actionFailed": "Pazaryeri eylemi tamamlanamadı. Tekrar deneyin.",
  "marketplace.thisAgent": "bu ajan",
} as const satisfies PartialTranslation<typeof source>;
