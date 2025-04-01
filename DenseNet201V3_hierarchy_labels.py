import os
from PIL import Image
import torch
import torch.nn as nn
import torch.optim as optim
from torch.optim.lr_scheduler import StepLR, ReduceLROnPlateau
from torch.utils.data import Dataset, DataLoader
from torchvision import transforms, models
import matplotlib.pyplot as plt
from sklearn.metrics import confusion_matrix
import numpy as np

# -----------------------------
# Configuration Flags
# -----------------------------
use_data_augmentation = False         # If False, minimal preprocessing is applied.
use_pretrained = False                # If False, DenseNet-201 is initialized randomly.
use_lr_scheduler = False              # (For non-base training regime)
use_base_model_training = True        # When True, use SGD with momentum, orthogonal initializer, and ReduceLROnPlateau scheduler.
high_num_epochs = 100                 # Set a high maximum epoch count.
early_stop_patience = 5               # Early stop if validation loss does not improve for these many epochs.

# -----------------------------
# Weight Initialization Function (Orthogonal)
# -----------------------------
def init_weights(m):
    if isinstance(m, (nn.Conv2d, nn.Linear)):
        if hasattr(m, 'weight') and m.weight is not None:
            nn.init.orthogonal_(m.weight)
        if hasattr(m, 'bias') and m.bias is not None:
            nn.init.constant_(m.bias, 0)

# -----------------------------
# Helper function to compute top-k correct counts
# -----------------------------
def accuracy_counts(output, target, topk=(1, 5)):
    """
    Returns number of correct predictions for each k in topk and the batch size.
    """
    maxk = max(topk)
    batch_size = target.size(0)
    # Get the top maxk predictions
    _, pred = output.topk(maxk, 1, True, True)  # shape: [batch_size, maxk]
    pred = pred.t()  # shape: [maxk, batch_size]
    correct = pred.eq(target.view(1, -1).expand_as(pred))
    # Count correct predictions for each k
    correct_top1 = correct[:1].reshape(-1).float().sum(0).item()
    correct_top5 = correct[:maxk].reshape(-1).float().sum(0).item()  # Works even if maxk < 5
    return correct_top1, correct_top5, batch_size

# -----------------------------
# Dataset definition
# -----------------------------
class YogaPoseDatasetMultiTask(Dataset):
    """
    Expects each row in the text file to be:
      <folder>/<file>,<L1 label>,<L2 label>,<L3 label>
    Builds sorted lists for each level and a hierarchy mapping (L3 -> (L1, L2)).
    """
    def __init__(self, txt_file, root_dir, transform=None):
        self.root_dir = root_dir
        self.transform = transform
        self.data = []  # list of tuples: (full_image_path, L1, L2, L3) as strings
        with open(txt_file, 'r') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                parts = line.split(',')
                if len(parts) != 4:
                    continue
                image_rel_path, l1, l2, l3 = parts
                full_path = os.path.join(root_dir, image_rel_path)
                self.data.append((full_path, l1, l2, l3))
        
        # Build sorted unique label lists for each level
        self.labels1 = sorted(list({l1 for (_, l1, _, _) in self.data}))
        self.labels2 = sorted(list({l2 for (_, _, l2, _) in self.data}))
        self.labels3 = sorted(list({l3 for (_, _, _, l3) in self.data}))
        
        # Build hierarchical mapping: for each L3 index, store its corresponding (L1 index, L2 index)
        self.hierarchy_mapping = {}
        for (_, l1, l2, l3) in self.data:
            l1_idx = self.labels1.index(l1)
            l2_idx = self.labels2.index(l2)
            l3_idx = self.labels3.index(l3)
            if l3_idx in self.hierarchy_mapping:
                if self.hierarchy_mapping[l3_idx] != (l1_idx, l2_idx):
                    print(f"Warning: Inconsistent hierarchy for L3 label '{l3}': {self.hierarchy_mapping[l3_idx]} vs {(l1_idx, l2_idx)}")
            else:
                self.hierarchy_mapping[l3_idx] = (l1_idx, l2_idx)
        
    def __len__(self):
        return len(self.data)
    
    def __getitem__(self, index):
        image_path, l1, l2, l3 = self.data[index]
        try:
            image = Image.open(image_path).convert('RGB')
        except Exception as e:
            print(f"Error loading image {image_path}: {e}. Returning a black image.")
            image = Image.new('RGB', (224, 224))
        # Apply provided transform or a default transform to ensure consistent size.
        if self.transform:
            image = self.transform(image)
        else:
            default_transform = transforms.Compose([
                transforms.Resize((224, 224)),
                transforms.ToTensor()
            ])
            image = default_transform(image)
        # Get label indices
        l1_idx = self.labels1.index(l1)
        l2_idx = self.labels2.index(l2)
        l3_idx = self.labels3.index(l3)
        return image, l1_idx, l2_idx, l3_idx

# -----------------------------
# Model Definitions
# -----------------------------
# Hierarchical Architecture (dual-head network)
class HierarchicalDenseNet201_V2(nn.Module):
    def __init__(self, num_classes1, num_classes2, num_classes3):
        super(HierarchicalDenseNet201_V2, self).__init__()
        densenet = models.densenet201(pretrained=use_pretrained)
        features_list = list(densenet.features.children())
        # Branch: up to DenseBlock3
        self.branch_features = nn.Sequential(*features_list[:9])
        # Main branch: remaining layers
        self.main_features = nn.Sequential(*features_list[9:])
        # Head layers for branch and main features
        self.branch_head = nn.Sequential(
            nn.BatchNorm2d(1792),
            nn.ReLU(inplace=True)
        )
        self.main_head = nn.Sequential(
            nn.BatchNorm2d(1920),
            nn.ReLU(inplace=True)
        )
        self.global_pool = nn.AdaptiveAvgPool2d((1, 1))
        self.fc_level1 = nn.Linear(1792, num_classes1)  # For L1 classification
        self.fc_level2 = nn.Linear(1792, num_classes2)  # For L2 classification
        self.fc_level3 = nn.Linear(1920, num_classes3)  # For L3 classification

    def forward(self, x):
        x_branch = self.branch_features(x)
        x_main = self.main_features(x_branch)
        branch_feat = self.branch_head(x_branch)
        branch_feat = self.global_pool(branch_feat).view(x.size(0), -1)
        main_feat = self.main_head(x_main)
        main_feat = self.global_pool(main_feat).view(x.size(0), -1)
        out1 = self.fc_level1(branch_feat)
        out2 = self.fc_level2(branch_feat)
        out3 = self.fc_level3(main_feat)
        return out1, out2, out3
    
# -----------------------------
# Training Function
# -----------------------------
def train_model(model, train_loader, test_loader, device, num_epochs=15):
    criterion = nn.CrossEntropyLoss()

    # Choose optimizer and scheduler based on training regime.
    if use_base_model_training:
        optimizer = optim.SGD(model.parameters(), lr=0.003, momentum=0.9)
        scheduler = ReduceLROnPlateau(optimizer, mode='min', factor=0.1, patience=2, verbose=True)
    else:
        optimizer = optim.Adam(model.parameters(), lr=1e-4)
        if use_lr_scheduler:
            scheduler = StepLR(optimizer, step_size=3, gamma=0.5)

    best_val_loss = float('inf')
    early_stop_counter = 0

    # To record training history
    train_loss_history = []
    train_top1_history = []
    train_top5_history = []
    test_loss_history = []
    test_top1_history = []
    test_top5_history = []

    for epoch in range(num_epochs):
        model.train()
        running_loss = 0.0
        total_samples = 0

        # Initialize accumulators for training accuracies
        total_top1_1, total_top5_1 = 0, 0
        total_top1_2, total_top5_2 = 0, 0
        total_top1_3, total_top5_3 = 0, 0
        total_samples_branch = 0

        print(f"\nStarting Epoch {epoch+1}/{num_epochs}")
        for batch_idx, data in enumerate(train_loader):
            optimizer.zero_grad()
            images = data[0].to(device)
            batch_size = images.size(0)
            total_samples += batch_size

            l1, l2, l3 = data[1].to(device), data[2].to(device), data[3].to(device)
            out1, out2, out3 = model(images)
            loss1 = criterion(out1, l1)
            loss2 = criterion(out2, l2)
            loss3 = criterion(out3, l3)
            loss = loss1 + loss2 + loss3

            # Calculate Top-1 and Top-5 correct counts for each branch
            correct_top1_1, correct_top5_1, _ = accuracy_counts(out1, l1, topk=(1,5))
            correct_top1_2, correct_top5_2, _ = accuracy_counts(out2, l2, topk=(1,5))
            correct_top1_3, correct_top5_3, _ = accuracy_counts(out3, l3, topk=(1,5))

            total_top1_1 += correct_top1_1
            total_top5_1 += correct_top5_1
            total_top1_2 += correct_top1_2
            total_top5_2 += correct_top5_2
            total_top1_3 += correct_top1_3
            total_top5_3 += correct_top5_3
            total_samples_branch += batch_size

            loss.backward()
            optimizer.step()
            running_loss += loss.item() * batch_size

            if batch_idx % 50 == 0:
                print(f"Epoch {epoch+1} Batch {batch_idx}: Loss {loss.item():.4f}")
                
        epoch_loss = running_loss / total_samples

        # Compute training accuracies
        acc1_top1 = 100.0 * total_top1_1 / total_samples_branch
        acc1_top5 = 100.0 * total_top5_1 / total_samples_branch
        acc2_top1 = 100.0 * total_top1_2 / total_samples_branch
        acc2_top5 = 100.0 * total_top5_2 / total_samples_branch
        acc3_top1 = 100.0 * total_top1_3 / total_samples_branch
        acc3_top5 = 100.0 * total_top5_3 / total_samples_branch
        print(f"Epoch {epoch+1}: Train Loss {epoch_loss:.4f}")
        print(f"  L1  -> Top-1: {acc1_top1:.2f}%, Top-5: {acc1_top5:.2f}%")
        print(f"  L2  -> Top-1: {acc2_top1:.2f}%, Top-5: {acc2_top5:.2f}%")
        print(f"  L3  -> Top-1: {acc3_top1:.2f}%, Top-5: {acc3_top5:.2f}%")
        current_train_top1, current_train_top5 = acc3_top1, acc3_top5

        train_loss_history.append(epoch_loss)
        train_top1_history.append(current_train_top1)
        train_top5_history.append(current_train_top5)

        # Evaluate on test set
        model.eval()
        test_loss = 0.0
        test_samples = 0

        test_top1_1, test_top5_1 = 0, 0
        test_top1_2, test_top5_2 = 0, 0
        test_top1_3, test_top5_3 = 0, 0
        test_samples_branch = 0

        with torch.no_grad():
            for data in test_loader:
                images = data[0].to(device)
                batch_size = images.size(0)
                test_samples += batch_size

                l1, l2, l3 = data[1].to(device), data[2].to(device), data[3].to(device)
                out1, out2, out3 = model(images)
                loss1 = criterion(out1, l1)
                loss2 = criterion(out2, l2)
                loss3 = criterion(out3, l3)
                loss = loss1 + loss2 + loss3

                ct1, ct5, _ = accuracy_counts(out1, l1, topk=(1,5))
                test_top1_1 += ct1
                test_top5_1 += ct5
                ct1, ct5, _ = accuracy_counts(out2, l2, topk=(1,5))
                test_top1_2 += ct1
                test_top5_2 += ct5
                ct1, ct5, _ = accuracy_counts(out3, l3, topk=(1,5))
                test_top1_3 += ct1
                test_top5_3 += ct5
                test_samples_branch += batch_size


                test_loss += loss.item() * batch_size

        avg_test_loss = test_loss / test_samples

        test_acc1_top1 = 100.0 * test_top1_1 / test_samples_branch
        test_acc1_top5 = 100.0 * test_top5_1 / test_samples_branch
        test_acc2_top1 = 100.0 * test_top1_2 / test_samples_branch
        test_acc2_top5 = 100.0 * test_top5_2 / test_samples_branch
        test_acc3_top1 = 100.0 * test_top1_3 / test_samples_branch
        test_acc3_top5 = 100.0 * test_top5_3 / test_samples_branch
        print(f"Epoch {epoch+1}: Test Loss {avg_test_loss:.4f}")
        print(f"  L1  -> Top-1: {test_acc1_top1:.2f}%, Top-5: {test_acc1_top5:.2f}%")
        print(f"  L2  -> Top-1: {test_acc2_top1:.2f}%, Top-5: {test_acc2_top5:.2f}%")
        print(f"  L3  -> Top-1: {test_acc3_top1:.2f}%, Top-5: {test_acc3_top5:.2f}%")
        current_test_top1, current_test_top5 = test_acc3_top1, test_acc3_top5

        test_loss_history.append(avg_test_loss)
        test_top1_history.append(current_test_top1)
        test_top5_history.append(current_test_top5)

        # Scheduler step
        if use_base_model_training:
            scheduler.step(avg_test_loss)
        elif use_lr_scheduler:
            scheduler.step()

        # Early stopping: if validation loss did not improve, count epochs and break if exceeded.
        if avg_test_loss < best_val_loss:
            best_val_loss = avg_test_loss
            early_stop_counter = 0
        else:
            early_stop_counter += 1
            if early_stop_counter >= early_stop_patience:
                print(f"Early stopping triggered at epoch {epoch+1}")
                break

        # Clear GPU cache at end of epoch
        torch.cuda.empty_cache()

    history = {
        'train_loss': train_loss_history,
        'train_top1': train_top1_history,
        'train_top5': train_top5_history,
        'test_loss': test_loss_history,
        'test_top1': test_top1_history,
        'test_top5': test_top5_history
    }
    return model, history

# -----------------------------
# Plotting functions
# -----------------------------
def plot_results(history, all_preds, all_labels, class_names):
    epochs = range(1, len(history['train_loss']) + 1)
    
    # Plot Loss vs. Epochs
    plt.figure()
    plt.plot(epochs, history['train_loss'], label='Train Loss')
    plt.plot(epochs, history['test_loss'], label='Test Loss')
    plt.xlabel("Epoch")
    plt.ylabel("Loss")
    plt.title("Loss vs. Epochs")
    plt.legend()
    plt.show()
    
    # Plot Top-1 Accuracy vs. Epochs
    plt.figure()
    plt.plot(epochs, history['train_top1'], label='Train Top-1')
    plt.plot(epochs, history['test_top1'], label='Test Top-1')
    plt.xlabel("Epoch")
    plt.ylabel("Top-1 Accuracy (%)")
    plt.title("Top-1 Accuracy vs. Epochs")
    plt.legend()
    plt.show()

    # Plot Top-5 Accuracy vs. Epochs
    plt.figure()
    plt.plot(epochs, history['train_top5'], label='Train Top-5')
    plt.plot(epochs, history['test_top5'], label='Test Top-5')
    plt.xlabel("Epoch")
    plt.ylabel("Top-5 Accuracy (%)")
    plt.title("Top-5 Accuracy vs. Epochs")
    plt.legend()
    plt.show()

    # Compute and plot confusion matrix
    cm = confusion_matrix(all_labels, all_preds)
    plt.figure(figsize=(8, 6))
    plt.imshow(cm, interpolation='nearest', cmap=plt.cm.Blues)
    plt.title("Confusion Matrix")
    plt.colorbar()
    tick_marks = np.arange(len(class_names))
    plt.xticks(tick_marks, class_names, rotation=45)
    plt.yticks(tick_marks, class_names)
    plt.xlabel("Predicted Label")
    plt.ylabel("True Label")
    plt.tight_layout()
    plt.show()

# -----------------------------
# Main Training Script
# -----------------------------
def main_train():
    # Update these paths as needed
    train_txt = 'filtered_yoga_train.txt'
    test_txt = 'filtered_yoga_test.txt'
    root_dir = ''
    
    # Set transforms
    if use_data_augmentation:
        train_transform = transforms.Compose([
            transforms.Resize((224, 224)),
            transforms.RandomHorizontalFlip(),
            transforms.RandomRotation(15),
            transforms.ColorJitter(brightness=0.2, contrast=0.2, saturation=0.2),
            transforms.ToTensor(),
            transforms.Normalize(mean=[0.485, 0.456, 0.406],
                                 std=[0.229, 0.224, 0.225])
        ])
    else:
        train_transform = transforms.Compose([
            transforms.Resize((224, 224)),
            transforms.ToTensor(),
            transforms.Normalize(mean=[0.485, 0.456, 0.406],
                                 std=[0.229, 0.224, 0.225])
        ])
    test_transform = transforms.Compose([
        transforms.Resize((224, 224)),
        transforms.ToTensor(),
        transforms.Normalize(mean=[0.485, 0.456, 0.406],
                                std=[0.229, 0.224, 0.225])
    ])
    
    train_dataset = YogaPoseDatasetMultiTask(train_txt, root_dir, transform=train_transform)
    test_dataset = YogaPoseDatasetMultiTask(test_txt, root_dir, transform=test_transform)
    
    batch_size = 32
    num_workers = 0
    train_loader = DataLoader(train_dataset, batch_size=batch_size, shuffle=True,
                              num_workers=num_workers, pin_memory=True)
    test_loader = DataLoader(test_dataset, batch_size=batch_size, shuffle=False,
                             num_workers=num_workers, pin_memory=True)
    
    print(f"L1 classes: {len(train_dataset.labels1)}")
    print(f"L2 classes: {len(train_dataset.labels2)}")
    print(f"L3 classes: {len(train_dataset.labels3)}")
    print("Hierarchical mapping (L3 index -> (L1 index, L2 index)):")
    for l3, (l1, l2) in train_dataset.hierarchy_mapping.items():
        print(f"{l3} -> ({l1}, {l2})")
    
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    model = HierarchicalDenseNet201_V2(num_classes1=len(train_dataset.labels1),
                                        num_classes2=len(train_dataset.labels2),
                                        num_classes3=len(train_dataset.labels3))
    model = model.to(device)
    
    # Initialize all weights with the orthogonal initializer if using base model training.
    if use_base_model_training:
        model.apply(init_weights)
    
    trained_model, history = train_model(model, train_loader, test_loader, device, num_epochs=high_num_epochs)
    torch.save(trained_model.state_dict(), 'base_no_aug.pth')
    print("Model saved as base_no_aug.pth")
    
    # After training, compute predictions on test set for confusion matrix plotting.
    all_preds = []
    all_labels = []
    trained_model.eval()
    with torch.no_grad():
        for data in test_loader:
            images = data[0].to(device)
            labels = data[3].to(device)
            _, _, out3 = trained_model(images)
            outputs = out3
            _, preds = torch.max(outputs, 1)
            all_preds.extend(preds.cpu().numpy())
            all_labels.extend(labels.cpu().numpy())
    
    # Plot training curves and confusion matrix.
    # (For the confusion matrix, we use the L3 label classes.)
    plot_results(history, all_preds, all_labels, train_dataset.labels3)

if __name__ == '__main__':
    main_train()
    print("Training complete.\nModel saved successfully.")
